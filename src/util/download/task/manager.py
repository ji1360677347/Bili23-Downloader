from util.common.data import reversed_video_quality_map, reversed_audio_quality_map, video_codec_str_map
from util.common import signal_bus, config, safe_remove, get_timestamp_ms, Translator
from util.parse.episode.tree import EpisodeData, Attribute
from util.common.enum import DownloadStatus, DownloadType
from util.thread import GlobalThreadPoolTask
from util.format import FileNameFormatter
from util.format.time import Time

from ..cover.manager import cover_manager
from .reparse_worker import ReparseWorker
from .db import TaskDatabase
from .info import TaskInfo

from pathlib import Path
from typing import List
from uuid import uuid4
import json
import re

class TaskManager:
    def __init__(self):
        self.db_manager = TaskDatabase()

        signal_bus.download.create_task.connect(self.create)

    def __episode_info_to_task_info(self, episode_info: dict) -> TaskInfo:
        task_info = TaskInfo()

        # BasicInfo
        task_info.Basic.task_id = str(uuid4())
        task_info.Basic.cover_id = cover_manager.arrange_cover_id(episode_info.get("cover", ""))
        task_info.Basic.show_title = episode_info.get("title", "")
        task_info.Basic.created_time = get_timestamp_ms()
        
        # DownloadInfo
        task_info.Download.status = DownloadStatus.QUEUED

        attribute = episode_info.get("attribute", 0)
        if attribute & Attribute.OPUS_BIT:
            # 动态图集只下载图片本身，强制使用视频通道传输并跳过合并
            task_info.Download.type = DownloadType.VIDEO
            task_info.Download.merge_video_audio = False
            task_info.Download.keep_original_files = False
        else:
            task_info.Download.type = self.__determine_download_type()
            task_info.Download.merge_video_audio = config.merge_video_audio
            task_info.Download.keep_original_files = config.keep_original_files

        task_info.Download.video_quality_id = config.video_quality_id
        task_info.Download.audio_quality_id = config.audio_quality_id
        task_info.Download.video_codec_id = config.video_codec_id

        # EpisodeInfo
        task_info.Episode.from_dict(self.__update_episode_info(episode_info))

        # FileNameInfo
        self.__update_file_name_info(task_info)

        return task_info

    def __determine_download_type(self):
        # 确定下载类型
        attr_dict = {
            DownloadType.VIDEO: config.download_video_stream,
            DownloadType.AUDIO: config.download_audio_stream,
            DownloadType.DANMAKU: config.get(config.download_danmaku),
            DownloadType.SUBTITLE: config.get(config.download_subtitle),
            DownloadType.COVER: config.get(config.download_cover),
            DownloadType.METADATA: config.get(config.download_metadata)
        }

        type = 0

        for attr, enabled in attr_dict.items():
            if enabled:
                type |= attr

        return type

    def __update_episode_info(self, episode_info: dict):
        extra_data = EpisodeData.get_episode_data(episode_info.get("episode_id", ""))

        title = episode_info.get("title", "")

        attr = episode_info.get("attribute", 0)

        episode_info["leaf_title"] = title

        if attr & Attribute.BANGUMI_BIT != 0 or attr & Attribute.CHEESE_BIT != 0:
            episode_info["episode_title"] = title

        data = {
            **episode_info,
            **extra_data,
            **episode_info.get("related_titles", {}),
            **episode_info.get("uploader_info", {}),
            "number": self.__arrange_number()
        }

        # 过滤文件系统非法字符
        self.__filter_illegal_characters(data)

        return data

    def __update_file_name_info(self, task_info: TaskInfo):
        attr = task_info.Episode.attribute

        # OPUS / SPACE 类目使用统一的 <发布时间>_<标题>[_image_<n>] 命名
        if attr & Attribute.OPUS_BIT:
            task_info.File.name = self.__build_opus_name(task_info)
            task_info.File.download_path = config.get(config.download_path)
            task_info.File.folder = self.__build_uploader_folder(task_info)
            if not task_info.File.video_file_ext:
                task_info.File.video_file_ext = self.__guess_image_ext(task_info.Episode.url)
            return

        if attr & Attribute.SPACE_BIT:
            task_info.File.name = self.__build_space_video_name(task_info)
            task_info.File.download_path = config.get(config.download_path)
            task_info.File.folder = self.__build_uploader_folder(task_info)
            return

        formatter = FileNameFormatter()
        formatter.set_variable_data(task_info)

        if config.target_naming_rule_id is not None:
            formatter.set_rule(formatter.get_rule_by_id(config.target_naming_rule_id))

        path = Path(formatter.format())

        task_info.File.name = str(path.name)

        task_info.File.download_path = config.get(config.download_path)
        task_info.File.folder = str(path.parent)

    @staticmethod
    def __format_pub_date(ts) -> str:
        if not ts:
            return ""
        try:
            return Time.format_timestamp(ts, "%Y-%m-%d %H.%M.%S")
        except Exception:
            return ""

    def __build_opus_name(self, task_info: TaskInfo) -> str:
        date_part = self.__format_pub_date(task_info.Episode.pubtime or 0)
        title = task_info.Episode.parent_title or task_info.Episode.collection_title or ""
        index = task_info.Episode.episode_number
        parts = [p for p in (date_part, title, f"image_{index}") if p]
        return "_".join(parts)

    def __build_space_video_name(self, task_info: TaskInfo) -> str:
        date_part = self.__format_pub_date(task_info.Episode.pubtime or 0)
        title = task_info.Episode.leaf_title or ""
        parts = [p for p in (date_part, title) if p]
        return "_".join(parts)

    def __build_uploader_folder(self, task_info: TaskInfo) -> str:
        owner = task_info.Episode.space_owner or ""
        owner_id = task_info.Episode.space_owner_id or 0
        if owner_id and owner:
            return f"{owner_id}_{owner}"
        if owner_id:
            return str(owner_id)
        return ""

    @staticmethod
    def __guess_image_ext(url: str) -> str:
        if not url:
            return "jpg"
        clean = url.split("?")[0].split("@")[0].lower()
        for ext in ("jpg", "jpeg", "png", "gif", "webp", "bmp"):
            if clean.endswith("." + ext):
                return ext
        return "jpg"

    def __task_already_downloaded(self, task_info: TaskInfo) -> bool:
        """根据预测的最终落盘路径，判断该任务是否已经下载过（增量下载）。
        OVERWRITE 模式下不做跳过，让用户的"覆盖"语义优先。"""
        from util.common.enum import FileConflictResolution
        if config.get(config.file_conflict_resolution) == FileConflictResolution.OVERWRITE:
            return False

        if not task_info.File.name:
            return False

        base = Path(task_info.File.download_path or "", task_info.File.folder or "")
        attr = task_info.Episode.attribute

        candidates: list[str] = []
        if attr & Attribute.OPUS_BIT:
            primary = task_info.File.video_file_ext or "jpg"
            for ext in [primary, "jpg", "jpeg", "png", "gif", "webp", "bmp"]:
                if ext and ext not in candidates:
                    candidates.append(ext)
        else:
            video_container = config.get(config.video_container)
            primary_ext = getattr(video_container, "value", "mp4") or "mp4"
            for ext in [primary_ext, "mp4", "mkv", "flv", "m4a", "mp3"]:
                if ext and ext not in candidates:
                    candidates.append(ext)

        for ext in candidates:
            if (base / f"{task_info.File.name}.{ext}").exists():
                return True
        return False

    def __task_identity_keys(self, task_info: TaskInfo) -> set[str]:
        keys = set()

        if task_info.Episode.url:
            keys.add(f"url:{task_info.Episode.url}")
        if task_info.Episode.bvid and task_info.Episode.cid:
            keys.add(f"bvid_cid:{task_info.Episode.bvid}:{task_info.Episode.cid}")
        if task_info.Episode.aid and task_info.Episode.cid:
            keys.add(f"aid_cid:{task_info.Episode.aid}:{task_info.Episode.cid}")
        if task_info.Episode.ep_id:
            keys.add(f"ep:{task_info.Episode.ep_id}")
        if task_info.File.name:
            base = Path(task_info.File.download_path or "", task_info.File.folder or "", task_info.File.name)
            keys.add(f"path:{str(base).casefold()}")

        return keys

    def __get_existing_download_task_keys(self) -> set[str]:
        keys = set()

        try:
            results = self.db_manager.query("""
                SELECT 
                    json_extract(data, '$.Episode.url'),
                    json_extract(data, '$.Episode.bvid'),
                    json_extract(data, '$.Episode.cid'),
                    json_extract(data, '$.Episode.aid'),
                    json_extract(data, '$.Episode.ep_id'),
                    json_extract(data, '$.File.download_path'),
                    json_extract(data, '$.File.folder'),
                    json_extract(data, '$.File.name')
                FROM download_task
            """)
            
            for r in results:
                url, bvid, cid, aid, ep_id, download_path, folder, name = r
                if url:
                    keys.add(f"url:{url}")
                if bvid and cid:
                    keys.add(f"bvid_cid:{bvid}:{cid}")
                if aid and cid:
                    keys.add(f"aid_cid:{aid}:{cid}")
                if ep_id and ep_id != 0 and ep_id != '0':
                    keys.add(f"ep:{ep_id}")
                if name:
                    base = Path(download_path or "", folder or "", name)
                    keys.add(f"path:{str(base).casefold()}")
        except Exception:
            import logging
            logging.getLogger(__name__).exception("优化查询排重 Key 失败，回退到原逻辑")
            for entry in self.db_manager.query_all_downloading_tasks():
                try:
                    task_info = TaskInfo()
                    task_info.from_dict(json.loads(entry[0]))
                    keys.update(self.__task_identity_keys(task_info))
                except Exception:
                    continue

        return keys

    def __check_reparse_needed(self, episode_info: dict):
        if episode_info.get("attribute", 0) & Attribute.NEED_PARSE_BIT:
            worker = ReparseWorker(episode_info)
            GlobalThreadPoolTask.run(worker)

            return True
        
        return False

    def __filter_illegal_characters(self, episode_info: dict):
        title_list = [
            "leaf_title", 
            "parent_title",
            "section_title",
            "collection_title",
            "series_title",
            "season_title",
            "episode_title",
            "favorites_owner",
            "space_owner"
        ]

        for title in title_list:
            if title in episode_info:
                # 过滤文件系统非法字符
                episode_info[title] = re.sub(r'[\/\\\:\*\?\"\<\>\|]', '_', episode_info.get(title, ""))

    def __arrange_number(self):
        config.current_starting_number += 1

        return config.current_starting_number - 1

    def create(self, episode_info_list: List[dict]):
        task_info_list = []

        skipped_existing = 0
        skipped_duplicate = 0
        existing_task_keys = self.__get_existing_download_task_keys()
        new_task_keys = set()

        for episode_info in episode_info_list:
            if self.__check_reparse_needed(episode_info):
                continue

            task_info = self.__episode_info_to_task_info(episode_info)

            if self.__task_already_downloaded(task_info):
                skipped_existing += 1
                continue

            task_keys = self.__task_identity_keys(task_info)
            if task_keys & (existing_task_keys | new_task_keys):
                skipped_duplicate += 1
                continue

            task_info_list.append(task_info)
            new_task_keys.update(task_keys)

        if skipped_existing:
            import logging
            logging.getLogger(__name__).info("增量下载：跳过 %d 个已存在的文件", skipped_existing)
        if skipped_duplicate:
            import logging
            logging.getLogger(__name__).info("队列去重：跳过 %d 个已在下载队列中的任务", skipped_duplicate)

        if task_info_list:
            # 存储到数据库，并添加到下载列表
            self.db_manager.add_tasks(task_info_list)

            signal_bus.download.add_to_downloading_list.emit(task_info_list)
            signal_bus.download.auto_manage_concurrent_downloads.emit()

    def query(self, completed: bool = False) -> List[TaskInfo]:
        result = self.db_manager.query_tasks(completed)

        task_info_list = []

        for entry in result:
            data = entry[0]  # 获取 data 列

            task_info = TaskInfo()
            task_info.from_dict(json.loads(data))

            task_info_list.append(task_info)

        return task_info_list

    def update(self, task_info: TaskInfo):
        self.db_manager.update_task(task_info)

    def delete(self, task_info: TaskInfo, completed: bool = False):
        self.db_manager.delete_task(task_info.Basic.task_id, completed)

    def cancel(self, task_info: TaskInfo):
        signal_bus.download.remove_from_downloading_list.emit(task_info)

        self.delete(task_info)
        
        self._removeTemporaryFiles(task_info)

    def mark_as_completed(self, task_info: TaskInfo):
        self.delete(task_info)

        self.db_manager.add_tasks([task_info], completed = True)

    def reset(self, task_info: TaskInfo):
        # 重置下载状态为初始状态，适用于完全重新下载的场景
        task_info.Download.status = DownloadStatus.QUEUED

        task_info.Download.queue = []
        task_info.Download.files = {}
        task_info.Download.progress = 0
        task_info.Download.total_size = 0
        task_info.Download.downloaded_size = 0
        task_info.Download.speed = 0

        self._removeTemporaryFiles(task_info)

    def recreate(self, task_info: TaskInfo):
        self.db_manager.delete_task(task_info.Basic.task_id, completed = True)
        self.db_manager.add_tasks([task_info])

        signal_bus.download.add_to_downloading_list.emit([task_info])
        signal_bus.download.auto_manage_concurrent_downloads.emit()

    def _removeTemporaryFiles(self, task_info: TaskInfo):
        # 删除下载的临时文件
        safe_remove(Path(task_info.File.download_path, task_info.File.folder), *task_info.File.relative_files)

    def _update_media_info(self, task_info: TaskInfo):
        # 更新媒体信息相关的变量，以便在文件命名规则中使用
        if task_info.Download.video_quality_id != 200:
            video_quality = reversed_video_quality_map.get(task_info.Download.video_quality_id, "")

            task_info.Episode.video_quality = Translator.VIDEO_QUALITY(video_quality)

        if task_info.Download.audio_quality_id != 30300:
            audio_quality = reversed_audio_quality_map.get(task_info.Download.audio_quality_id, "")

            task_info.Episode.audio_quality = Translator.AUDIO_QUALITY(audio_quality)

        if task_info.Download.video_codec_id != 20:
            video_codec = video_codec_str_map.get(task_info.Download.video_codec_id, "")

            task_info.Episode.video_codec = video_codec

        self.__update_file_name_info(task_info)

task_manager = TaskManager()

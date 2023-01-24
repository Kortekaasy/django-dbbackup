"""
Save media files.
"""

import os
from pathlib import Path
import tarfile

from django.core.files.storage import Storage
from django.core.management.base import CommandError

from typing import Dict

from ... import utils
from ...storage import StorageError, get_storage
from ._base import BaseDbBackupCommand, make_option

from dbbackup import settings


class Command(BaseDbBackupCommand):
 
    help = """Backup media files, gather all in a tarball and encrypt or
    compress."""
    content_type = "media"
    content_storages: Dict[str, Storage] = settings.BACKUP_LOCATIONS

    option_list = BaseDbBackupCommand.option_list + (
        make_option(
            "-c",
            "--clean",
            help="Clean up old backup files",
            action="store_true",
            default=False,
        ),
        make_option(
            "-s",
            "--servername",
            help="Specify server name to include in backup filename",
        ),
        make_option(
            "-z",
            "--compress",
            help="Compress the archive",
            action="store_true",
            default=False,
        ),
        make_option(
            "-e",
            "--encrypt",
            help="Encrypt the backup files",
            action="store_true",
            default=False,
        ),
        make_option(
            "-o", "--output-filename", default=None, help="Specify filename on storage"
        ),
        make_option(
            "-O",
            "--output-path",
            default=None,
            help="Specify where to store on local filesystem",
        ),
    )

    @utils.email_uncaught_exception
    def handle(self, **options):
        self.verbosity = options.get("verbosity")
        self.quiet = options.get("quiet")
        self._set_logger_level()

        self.encrypt = options.get("encrypt", False)
        self.compress = options.get("compress", False)
        self.servername = options.get("servername")

        self.filename = options.get("output_filename")
        self.path = options.get("output_path")
        try:
            self.storage = get_storage()
            self.backup_mediafiles()
            if options.get("clean"):
                self._cleanup_old_backups(servername=self.servername)

        except StorageError as err:
            raise CommandError(err) from err

    def _explore_storage(self, content_storage: Storage):
        """Generator of all files contained in `content_storage`."""
        path = ""
        dirs = [path]
        while dirs:
            path = dirs.pop()
            subdirs, files = content_storage.listdir(path)
            for filename in files:
                yield os.path.join(path, filename)
            dirs.extend([os.path.join(path, subdir) for subdir in subdirs])

    def _create_tar(self, name):
        """Create TAR file."""
        fileobj = utils.create_spooled_temporary_file()
        mode = "w:gz" if self.compress else "w"
        tar_file = tarfile.open(name=name, fileobj=fileobj, mode=mode)
        for label, content_storage in self.content_storages.items():
            for content_filename in self._explore_storage(content_storage):
                tarinfo = tarfile.TarInfo(str(Path(label, content_filename)))
                content_file = content_storage.open(content_filename)
                tarinfo.size = len(content_file)
                self.logger.info("Adding %s - %s", tarinfo.name, tarinfo.path)
                tar_file.addfile(tarinfo, content_file)
        # Close the TAR for writing
        tar_file.close()
        return fileobj

    def backup_mediafiles(self):
        """
        Create backup file and write it to storage.
        """
        # Check for filename option
        if self.filename:
            filename = self.filename
        else:
            extension = f"tar{'.gz' if self.compress else ''}"
            filename = utils.filename_generate(
                extension, servername=self.servername, content_type=self.content_type
            )

        tarball = self._create_tar(filename)
        # Apply trans
        if self.encrypt:
            encrypted_file = utils.encrypt_file(tarball, filename)
            tarball, filename = encrypted_file

        self.logger.debug("Backup size: %s", utils.handle_size(tarball))
        # Store backup
        tarball.seek(0)
        if self.path is None:
            self.logger.info("Writing backup file to %s", filename)
            self.write_to_storage(tarball, filename)
        else:
            self.logger.info("Writing backup file to %s", self.path)
            self.write_local_file(tarball, self.path)

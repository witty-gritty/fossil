# This script is meant to be used as a manager for the fossil CLI.

import os
from os import path
import xml.etree.ElementTree as ET
import hashlib
from functools import partial
import sys
import tarfile
import configparser
import shutil
import prettytable

HOME = os.getenv('HOME')
PATH = '.fossil'
CONFIG = path.join(PATH, 'config.ini')
SNAPSHOTS = path.join(PATH, 'snapshots')
PROFILES = path.join(PATH, 'profiles')
BUFFER = path.join(PATH, 'buffer')

__doc__ = """A tool for backing up filesystems.
How this works?
This script is capable of backing up your files at certain stages of its lifetime.

You have profiles where you can add your files. Profiles help with grouping certain set of files
to track it independently. You can create, remove and rename a profile under this current version.
By Default, your profile will be named "current_profile".

Adding files to your profile just lets the profile keep a record of your file.
When you take a snapshot of your selected profile, the files with their current state will
be backed up under the "snapshots/" folder.

You can restore the snapshot to bring your system to the state mentioned in your snapshot. It means
that the deleted files will be restored and the current files will be rolled back to its previous
version.
"""

def init():
    """Initializes the file tree:
    ~  -|
        |- .fossil (dir)   -|
                            |- config.ini
                            |- snapshots (dir)
                            |- profiles (dir)  -|
                                                |- current_profile.xml
    """
    os.makedirs(SNAPSHOTS)
    os.mkdir(PROFILES)

    parser = configparser.ConfigParser()
    parser['DEFAULTS'] = {'DATABASE': path.join(PROFILES, 'current_profile.xml')}
    with open(CONFIG, mode='w', encoding='utf-8') as fobj:
        parser.write(fobj)

    root = ET.Element('data')
    tree = ET.ElementTree(root)
    tree.write(path.join(PROFILES, 'current_profile.xml'))

if not path.exists(PATH):
    init()

def check_database_integrity():
    """Perform checks to ensure the integrity of the database mentioned in the "config.ini" file.
    """
    if config_parser['DEFAULTS'].get('DATABASE') == '':
        print('Select a profile first')
        sys.exit()

config_parser = configparser.ConfigParser()
with open(CONFIG, mode='r', encoding='utf-8') as config_file:
    config_parser.read_file(config_file)
DATABASE = config_parser.get('DEFAULTS', 'DATABASE')

class File(ET.Element):
    """A preconfigured File Element with accessible properties,
    meant to represent a file in the file(xml) database.

    <file index = "0">
        <name> file_name </ name>
        <relpath> relative_path </ relpath>
        <sha256> file_hash <sha256>
    </ file>

    Returns:
        File (ET.Element): A pre-configured File Element(XML Element) with accessible properties.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self) -> None:
        super().__init__('file')

        self.set('index', '0')
        self._name = ET.SubElement(self, 'name')
        self._relpath = ET.SubElement(self, 'relpath')
        self._sha256 = ET.SubElement(self, 'sha256')

    @staticmethod
    def from_element(element: ET.Element) -> ET.Element:
        """Generate and return a file object from ElementTree.Element object.

        Args:
            element (ET.Element): ElementTree.Element which is also a file-like object.

        Returns:
            File (ET.Element): A pre-configured File Element(XML Element)
            with accessible properties.
        """
        file = File()

        file.index = element.get('index')
        file.name = element.find('name').text
        file.relpath = element.find('relpath').text
        file.sha256 = element.find('sha256').text

        return file

    @property
    def index(self):
        """Sets the index of the file.

        Returns:
            None
        """
        return self.get('index')
    @index.setter
    def index(self, index):
        self.set('index', f'{index}')

    @property
    def name(self):
        """Sets the name of the file.

        Returns:
            None
        """
        return self._name.text
    @name.setter
    def name(self, name):
        self._name.text = name

    @property
    def relpath(self):
        """Sets the relative path of the file.

        Returns:
            None
        """
        return self._relpath.text
    @relpath.setter
    def relpath(self, relpath):
        self._relpath.text = relpath

    @property
    def sha256(self):
        """Sets the hash of the file(Preferrably sha256).

        Returns:
            None
        """
        return self._sha256.text
    @sha256.setter
    def sha256(self, sha256):
        self._sha256.text = sha256

    def __repr__(self):
        return f"""
<file index="{self.index}">
    <name>{self.name}</ name>
    <relpath>{self.relpath}</ relpath>
    <sha256>{self.sha256}</ sha256>
</ file>
"""

class Profile:
    """The purpose of this class is to group similar profile manipulation functions.
    The profile mentioned here will be synced with the one mentioned in the "config.ini" file.

    Quick overview:
    - create_profile: create a profile.
    - remove_profile: remove a profile.
    - rename_profile: rename the currently selected profile.
    - select_profile: select a profile with their name.
    - list_profiles: list the profiles under "profiles/" directory.
    """
    @staticmethod
    def create_profile(name):
        """Create a profile under the "profiles" folder.
        A profile is an XML document to store file elements.

        Args:
            name (str): Profile name for the file.
        """
        if f'{name}.xml' in os.listdir(path.join(PATH, 'profiles')):
            return

        root = ET.Element('data')
        tree = ET.ElementTree(root)
        tree.write(path.join(PATH, 'profiles', f'{name}.xml'))

    @staticmethod
    def remove_profile(name):
        """Remove the profile from under the "profiles/" folder.

        Args:
            name (str): Name of the profile to be deleted.
        """
        if f'{name}.xml' in os.listdir(path.join(PATH, 'profiles')):
            os.remove(path.join(PATH, 'profiles', f'{name}.xml'))

        config_parser['DEFAULTS'] = {'DATABASE': ''}
        with open(path.join(PATH, 'config.ini'), mode='w', encoding='utf-8') as fobj:
            config_parser.write(fobj)

    @staticmethod
    def rename_profile(new_name):
        """Rename the currently selected profile(found in the config.ini file) with a new name.
        Rewrites the config.ini file to sync with the new name.

        Args:
            new_name (str): The new name of the profile.
        """
        check_database_integrity()
        if path.exists(path.join(PATH, 'profiles', f'{new_name}.xml')):
            return

        os.rename(DATABASE, path.join(PATH, 'profiles', f'{new_name}.xml'))
        config_parser['DEFAULTS'] = {'DATABASE': path.join(PATH, 'profiles', f'{new_name}.xml')}
        with open(path.join(PATH, 'config.ini'), open='w', encoding='utf-8') as fobj:
            config_parser.write(fobj)

    @staticmethod
    def select_profile(name):
        """Select a profile and update the config file.
        Newly added files are written in the selected profile.
        The profile mentioned must be created before selecting it.

        Args:
            name (str): Name of the profile to select.
        """
        if not f'{name}.xml' in os.listdir(path.join(PATH, 'profiles')):
            return

        config_parser['DEFAULTS'] = {'DATABASE': path.join(PATH, 'profiles', f'{name}.xml')}
        with open(path.join(PATH, 'config.ini'), mode='w', encoding='utf-8') as fobj:
            config_parser.write(fobj)

    @staticmethod
    def list_profiles():
        """List the profiles under "profiles/" dir.
        """
        for profiles in os.listdir(path.join(PATH, 'profiles')):
            print(profiles.split('.')[0])

class Database:
    """The sole purpose of this class is to group similar database IO functions.
    The database mentioned here is an XML database in accordance to the
    selected profile in the "config.ini" file under the "profiles/" directory.
    Files mentioned here are XML Elements representing the files.

    Quick overview:
    - add_file: add a file to the database.
    - remove_file: remove a file from the database.
    - list_files: list the files from the database.
    - take_snapshot: take a snapshot(backup) of the current state.
    - restore_snapshot: restore a snapshot to bring your system in that state.
    """
    @staticmethod
    def add_file(filepath):
        """Add an XML representation of a file to the currently selected profile.
        Files added are just tracked, not copied until a snapshot.
        If a file is deleted, it will simply not be included in the following snapshots.

        Args:
            filepath (str): Path to the file.
        """
        check_database_integrity()
        if not path.exists(filepath):
            return

        tree = ET.parse(DATABASE)
        root = tree.getroot()
        relpath = path.relpath(path.join(os.getcwd(), filepath), HOME)
        for elem in root.iterfind('file'):
            if relpath == elem.find('relpath').text:
                return

        file = File()
        file.index = len(root.findall('file'))
        file.name = path.basename(filepath)
        file.relpath = relpath

        root.append(file)
        tree.write(DATABASE)

    @staticmethod
    def remove_file(index):
        """Remove a file from the list of files under the currently
        selected profile(config.ini) by the help of a specified index.

        Args:
            index (int): An index to the file. List the files with list_files()
            to obtain their indices.
        """
        check_database_integrity()

        tree = ET.parse(DATABASE)
        root = tree.getroot()
        file_list = root.findall('file')

        if not file_list:
            return
        if index > len(file_list) - 1 or index < 0:
            return

        new_file_list = [file for file in file_list if not file.get('index') == str(index)]
        new_root = ET.Element('data')
        new_tree = ET.ElementTree(new_root)

        for i, elem in enumerate(new_file_list):
            file = File.from_element(elem)
            file.index = i
            new_root.append(file)

        new_tree.write(DATABASE)

    @staticmethod
    def list_files(field=None):
        """List the files from the currently selected profile(config.ini).

        Args:
            field (str, optional): List out a specific field. Defaults to None.

        fields: (name, path)
        """
        check_database_integrity()

        tree = ET.parse(DATABASE)
        root = tree.getroot()

        if field in ('name', 'path'):
            for elem in root.iterfind('file'):
                file = File.from_element(elem)
                if field == 'name':
                    print(file.name)
                if field == 'path':
                    print(file.relpath)
            return

        table = prettytable.PrettyTable(['Index', 'Filename', 'Path'])
        for elem in root.iterfind('file'):
            file = File.from_element(elem)
            table.add_row([file.index, file.name, file.relpath])
        table.set_style(prettytable.MARKDOWN)
        print(table)

    @staticmethod
    def take_snapshot():
        """Take a snapshot and store it under the "snapshots/" directory,
        nested again under a directory named after the currently selected profile.
        Snapshots are stored as tarball files.

        Process:
        - Files are hashed and the a new profile, now named "profile.xml",
        is updated with the new hashes, along with the old data.
        - Files, along with the profile file, are first copied to a buffer folder
        with file names similar to their hashes.
        - Files are copied to a tarball, along with the profile, and then simply
        saved under the snapshots/[profile]/ dir.
        - Finishing it off, the buffer directory with all its contents are purged.
        """
        check_database_integrity()
        if path.exists(BUFFER):
            shutil.rmtree(BUFFER)
        os.mkdir(BUFFER)

        tree = ET.parse(DATABASE)
        root = tree.getroot()
        new_root = ET.Element('data')
        new_tree = ET.ElementTree(new_root)

        for elem in root.iterfind('file'):
            file = File.from_element(elem)
            with open(file.relpath, mode='rb') as fobj:
                digest = hashlib.md5()
                for buf in iter(partial(fobj.read, 128), b''):
                    digest.update(buf)
            file.sha256 = digest.hexdigest()
            new_root.append(file)
            shutil.copy(file.relpath, path.join(BUFFER, file.sha256))
        new_tree.write(path.join(BUFFER, 'profile.xml'))

        dirname = path.basename(DATABASE).split('.')[0]
        if not path.exists(path.join(SNAPSHOTS, dirname)):
            os.mkdir(path.join(SNAPSHOTS, dirname))
        filename = path.join(SNAPSHOTS,
                            dirname,
                            f'{dirname}[{len(os.listdir(path.join(SNAPSHOTS, dirname)))}].tar')

        with tarfile.open(filename, mode='w', encoding='utf-8') as fobj:
            for elem in os.listdir(BUFFER):
                fobj.add(path.join(BUFFER, elem))

    @staticmethod
    def restore_snapshot(snapshot_filepath):
        """Restore a previously taken snapshot.

        Args:
            snapshot_filepath (str): A path to the snapshot.

        Process:
        - Contents of the snapshot are extracted under a buffer folder.
        - The profile file is read and files are then matched with their hashes,
        before copying them one by one to their appropriate paths. During this process,
        the whole file tree mentioned will be reconstructed, along with the missing dirs.
        - The buffer folder is purged.
        """
        if not path.exists(snapshot_filepath):
            print("Path doesn't exist")
            return

        if path.exists(BUFFER):
            shutil.rmtree(BUFFER)
        os.mkdir(BUFFER)
        with tarfile.open(path.join(BUFFER, 'snapshot.tar'), mode='r', encoding='utf-8') as fobj:
            fobj.extractall(HOME)

        tree = ET.parse(path.join(BUFFER, 'profile.xml'))
        root = tree.getroot()
        for elem in root.iterfind('file'):
            file = File.from_element(elem)
            if not path.exists(path.dirname(file.relpath)):
                os.makedirs(path.relpath(path.dirname(file.relpath), HOME))
            shutil.copy(path.join(BUFFER, file.sha256), file.relpath)

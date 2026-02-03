import os
import traceback
import shutil
from peewee import DoesNotExist, IntegrityError, prefetch

from cuemsutils.tools.StringSanitizer import StringSanitizer
from cuemsutils.tools.CopyMoveVersioned import CopyMoveVersioned
from cuemsutils.xml.Parsers import CuemsParser
from cuemsutils.xml.XmlReaderWriter import XmlReaderWriter
from cuemsutils.helpers import new_datetime, new_uuid
from cuemsutils.log import logged, Logger


from CuemsErrors import *
from CuemsDBModel import Project, Media, ProjectMedia




class CuemsDBProject(StringSanitizer):

    def __init__(self, settings_dict, db_connection):
        self.db = db_connection
        self.settings_dict = settings_dict
        try:
            self.tmp_path = self.settings_dict['tmp_path']
            self.library_path = settings_dict['library_path']
            self.script_file_name = settings_dict['script_file_name']
            self.script_schema_name = settings_dict['script_schema_name']
            self.projects_path = os.path.join(self.library_path, settings_dict['project_folder_name'])
            self.trash_path = os.path.join(self.library_path, settings_dict['trash_folder_name'], settings_dict['project_folder_name'])
            self.media_path = os.path.join(self.library_path, settings_dict['media_folder_name'])
        except KeyError as e:
            Logger.error(f'can not read settings {e}')
            raise e
    
    
    def get_project_unix_name(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            return project.unix_name
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))
        
    def load(self, uuid, include_trash=False):
        try:
            if not include_trash:
                project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            else:
                project = Project.get(Project.uuid==uuid)
            return self.load_xml(project.unix_name)
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def list(self):
        project_list = list()
        projects = Project.select().where(Project.in_trash == False).order_by(Project.created.desc())
        for project in projects:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'created': project.created, 'modified': project.modified} }
            project_list.append(project_dict)

        return project_list
    
    def list_trash(self):
        project_trash_list = list()
        projects_trash = Project.select().where(Project.in_trash == True).order_by(Project.created.desc())
        for project in projects_trash:
            project_dict = {str(project.uuid): {'name': project.name, 'unix_name': project.unix_name, 'created': project.created, 'modified': project.modified} }
            project_trash_list.append(project_dict)

        return project_trash_list

    def update(self, uuid, data):   #TODO: check uuid format
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

        try:
            del data['CuemsScript']['unix_name']
        except KeyError:
            pass

        # TEMPORARY FIX: Frontend doesn't send correct media duration, fix it from database
        # TODO: Remove this once frontend properly fetches duration via file_load_meta
        self._fix_media_durations(data)

        with self.db.atomic() as transaction:
            try:
                project.name=StringSanitizer.sanitize_name(data['CuemsScript']['name'])
                now = new_datetime()
                data['CuemsScript']['modified'] = now
                project.modified=now
                project.description=StringSanitizer.sanitize_text_size(data['CuemsScript']['description'])
                project.save()
                project_object = CuemsParser(data).parse()
                self.update_media_relations(project, project_object)
                self.save_xml(project.unix_name, project_object)
            except Exception as e:
                Logger.error("error: {} {} trying to update  project, rolling back database update".format(type(e), e))
                transaction.rollback()
                raise e
            
    # TEMPORARY FIX: Frontend doesn't send correct media duration
    # TODO: Remove this once frontend properly fetches duration via file_load_meta
    def _fix_media_durations(self, data):
        """Fix media durations in project data from database.
        
        The frontend sends duration as '00:00:00.000' even though the database
        has the correct duration. This method looks up each media file's duration
        from the database and updates it in the project data before saving.
        """
        try:
            cuelist = data.get('CuemsScript', {}).get('CueList', {})
            contents = cuelist.get('contents', [])
            self._fix_durations_recursive(contents)
        except Exception as e:
            logger.warning(f"Could not fix media durations: {e}")

    def _fix_durations_recursive(self, contents):
        """Recursively fix media durations in cue contents."""
        if not contents:
            return
            
        for item in contents:
            for cue_type in ['AudioCue', 'VideoCue', 'CueList']:
                if cue_type in item:
                    cue_data = item[cue_type]
                    if cue_type == 'CueList':
                        nested_contents = cue_data.get('contents', [])
                        self._fix_durations_recursive(nested_contents)
                    else:
                        media = cue_data.get('Media', {})
                        if media:
                            file_name = media.get('file_name')
                            if file_name:
                                try:
                                    db_media = Media.get(Media.unix_name == file_name)
                                    if db_media.duration:
                                        old_duration = media.get('duration', '00:00:00.000')
                                        media['duration'] = str(db_media.duration)
                                        if old_duration != media['duration']:
                                            logger.debug(f"Fixed duration for {file_name}: {old_duration} -> {media['duration']}")
                                except DoesNotExist:
                                    logger.warning(f"Media not found in database: {file_name}")

    def new(self, data, unix_name):

        try:
            unix_name = StringSanitizer.sanitize_dir_permit_increment(unix_name)
        except Exception as e:
            raise e
        
        try:
            project_uuid = str(new_uuid())
            data['CuemsScript']['id']= project_uuid
            now = new_datetime()
            data['CuemsScript']['created'] = now
            data['CuemsScript']['modified'] = now
        except KeyError as e:
            Logger.error("error: Missing {} ;trying to make new  project, rolling back database insert".format(e))
            raise e
        except Exception as e:
            Logger.error("error: {} {} ;trying to read  project data".format(type(e), e))
            raise e

        with self.db.atomic() as transaction:
            try:
                project = Project.create(uuid=project_uuid, unix_name=unix_name, name=StringSanitizer.sanitize_name(data['CuemsScript']['name']), description=StringSanitizer.sanitize_text_size(data['CuemsScript']['description']), created=now, modified=now)
                os.mkdir(os.path.join(self.projects_path, unix_name))
                Logger.debug('data is now: {}'.format(data))
                project_object = CuemsParser(data).parse()
                Logger.debug(f'project_object is now: {type(project_object)},{project_object}')
                self.add_media_relations(project, project_object)
                self.save_xml(unix_name, project_object)
                return project_uuid
            except IntegrityError as e:
                transaction.rollback()
                Logger.error("error: {} {} ;name or unix_name already exists, rolling back database insert".format(type(e), e))
                raise e
            except Exception as e:
                transaction.rollback()
                Logger.error("error: {} {} ;trying to make new  project, rolling back database insert".format(type(e), e))
                
                if os.path.exists(os.path.join(self.projects_path, unix_name)):
                    shutil.rmtree(os.path.join(self.projects_path, unix_name) )
                             
                raise e

    def duplicate(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            with self.db.atomic() as transaction:
                try:
                    new_unix_name = None
                    project_path = os.path.join(self.projects_path, project.unix_name)
                    new_unix_name = CopyMoveVersioned.copy_dir(project_path, self.projects_path, project.unix_name)
                    project.unix_name = new_unix_name
                    new_project_uuid = str(new_uuid())
                    project.uuid = new_project_uuid
                    project.name = project.name + ' - Copy'
                    project.modified=new_datetime()
                    project.save(force_insert=True)

                    dup_project= Project.get(Project.uuid==new_project_uuid)
                    data = self.load_xml(dup_project.unix_name)
                    project_object = CuemsParser(data).parse()
                    self.add_media_relations(dup_project, project_object)
                    return new_project_uuid
                except Exception as e:
                    Logger.error("error: {} {}; trying to duplicate  project, rolling back database update".format(type(e), e))
                    transaction.rollback()
                    if new_unix_name is None:  # if move or copy where not successful with don't need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.projects_path, new_unix_name)):
                        shutil.rmtree(os.path.join(self.projects_path, new_unix_name))
                    raise e
            
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def delete(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == False))
            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    file_path = os.path.join(self.projects_path, project.unix_name)
                    dest_filename = CopyMoveVersioned.move(file_path, self.trash_path, project.unix_name)
                    project.in_trash = True
                    project.save()
                    Logger.debug('updating instance in db: {}'.format(project))
                except Exception as e:
                    Logger.error("error: {} {}; trying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not successful with don't need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.trash_path, dest_filename)):
                        shutil.move( os.path.join(self.trash_path, dest_filename), os.path.join(self.projects_path, project.unix_name))
                    raise e

        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))
    
    def restore(self, uuid):
        try:
            project_trash = Project.get((Project.uuid==uuid) & (Project.in_trash == True))
        
            with self.db.atomic() as transaction:
                try:
                    dest_filename = None
                    project_path = os.path.join(self.trash_path, project_trash.unix_name)
                    dest_filename = CopyMoveVersioned.move(project_path, self.projects_path, project_trash.unix_name)
                    project_trash.in_trash = False
                    project_trash.save()
                    Logger.debug('updating instance in db: {}'.format(project_trash))
                except Exception as e:
                    Logger.error("error: {} {}; trying to move file to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    if dest_filename is None:  # if move or copy where not successful with don't need to clean and can end here forwarding the exception, else continue cleaning and then forward the exception
                        raise e
                    if os.path.exists(os.path.join(self.projects_path, dest_filename)):
                        shutil.move( os.path.join(self.projects_path, dest_filename), os.path.join(self.trash_path, project_path.unix_name))
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def delete_from_trash(self, uuid):
        try:
            project = Project.get((Project.uuid==uuid) & (Project.in_trash == True))

            with self.db.atomic() as transaction:
                try:
                    project_path = os.path.join(self.trash_path, project.unix_name)
                    project.delete_instance(recursive=True)
                    shutil.rmtree(project_path)  #non empty dir, must use rmtree
                    Logger.debug('deleting project from trash: {}'.format(project))
                except Exception as e:
                    Logger.error("error: {} {}; trying to delete project to trash, rolling back database".format(type(e), e))
                    transaction.rollback()
                    raise e
        except DoesNotExist:
            raise NonExistentItemError("item with uuid: {} does not exist".format(uuid))

    def add_media_relations(self, project, project_object):
        media_filenames_list = project_object.get_media_filenames()
        for media_name in media_filenames_list:
            media = Media.get(Media.unix_name==media_name)
            ProjectMedia.create( project=project, media=media, media_filename=media_name)    
    
    def update_media_relations(self, project, project_object):
        Logger.debug('updating media relations for project: {}'.format(project.unix_name))
        old_media_query = project.medias()
        old_media_dict = dict()
        Logger.debug('query done')
        for media in old_media_query:
            old_media_dict[media.unix_name] = str(media.uuid)
        old_media_list=list(old_media_dict.keys())
        Logger.debug('old media list: {}'.format(old_media_list))
        media_list = project_object.get_media_filenames()
        Logger.debug('media list: {}'.format(media_list))
         
        remove_set = set(old_media_list).difference(media_list)
        add_set = set(media_list).difference(old_media_list)

        Logger.debug('media remove list: {}'.format(remove_set))
        Logger.debug('media add list: {}'.format(add_set))

        if remove_set:
            for media_unix_name in remove_set:
                ProjectMedia.delete().where((ProjectMedia.project == project)&(ProjectMedia.media == old_media_dict[media_unix_name] )).execute() 

        if add_set:
            for media_unix_name in add_set:
                media = Media.select(Media.uuid).where(Media.unix_name==media_unix_name).get()
                ProjectMedia.create( project=project, media=media, media_filename=media_unix_name)  

    def update_projects_existed_media(self, project_uuid, media_filename):
        project_object = CuemsParser(self.load(project_uuid, include_trash=True)).parse()
        media_dict = project_object.get_media()
        matching_media_dict= dict()
        for cue_uuid, media_object in media_dict.items(): 
            for media_uiid, project_media_filename in media_object.items():
                if media_filename == project_media_filename:
                    matching_media_dict[cue_uuid] = media_object

        if matching_media_dict:
            Logger.debug('found cues with media filename: {} in project: {}'.format(media_filename, project_uuid))
            first_media_object = next(iter(matching_media_dict.values()))
            old_media_uuid = next(iter(first_media_object.keys()))
            # TODO: manage if  all media with same filename have the same uuid  SHOULD BE TRUE
            for cue_uuid, media in matching_media_dict.items():
                for media_uuid, media_filename in media.items():
                    if media_uuid != old_media_uuid:
                        Logger.warning('found different media uuid for same media filename: {} in project {},  cue {}, using first found: {}'.format(project_uuid, cue_uuid, media_filename))   
            try:
                self.update_existed_media_uuid(media_filename, old_media_uuid)
            except IntegrityError:
                Logger.warning('error updating media uuid for media filename: {} from project: {}. Media uuid inconsistency detected'.format(media_filename, project_uuid))
            self.deletele_mising_media_references(media_filename)
            project = Project.get(Project.uuid==project_uuid)
            self.update_media_relations(project, project_object)
        else:
            Logger.warning('no cues found for media filename: {}'.format(media_filename))


    def update_existed_media_uuid(self, media_filename, old_media_uuid):
            Logger.debug('updating media uuid for media filename: {} with old uuid: {}'.format(media_filename, old_media_uuid))
            try:
                media = Media.get(Media.unix_name==media_filename)
                with self.db.atomic() as transaction:
                    try:
                        Media.update(uuid=old_media_uuid).where(Media.unix_name == media_filename).execute()
                    except Exception as e:
                        Logger.error("error: {} {}; trying to update media uuid, rolling back database update".format(type(e), e))
                        transaction.rollback()
                        raise e
            except DoesNotExist:
                raise NonExistentItemError("item with unix_name: {} does not exist".format(media_filename)) 
              
    def deletele_mising_media_references(self, media_filename):
        Logger.debug('deleting missing media references for media filename: {}'.format(media_filename))
        missing_media_project_refs = ProjectMedia.delete().where(ProjectMedia.media_filename == media_filename and ProjectMedia.media_id.is_null()).execute()

    def save_xml(self, unix_name, project_object):

        writer = XmlReaderWriter(schema_name = self.script_schema_name, xmlfile = (os.path.join(self.projects_path, unix_name, self.script_file_name)))
        writer.write_from_object(project_object)


    def load_xml(self, unix_name):
        reader = XmlReaderWriter(schema_name = self.script_schema_name, xmlfile = (os.path.join(self.projects_path, unix_name, self.script_file_name)))
        return reader.read()

            

        




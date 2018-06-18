import sys, traceback, smtplib
# Import the email modules we'll need
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import base64 , json , re
import xmltodict
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import grequests
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
import time
import pycountry
import asyncio
from asyncio import coroutine
import hashlib
from deepdiff import DeepDiff
from pprint import pprint
from sortedcontainers import SortedDict
from gevent import monkey, sleep
monkey.patch_all()

class CordisProject:
  def __init__(self,database_connections, app, my_logger):
    self.say_my_name = 'CPD@erc.europa.eu'
    self.email_for_notifications = 'send_to_this@gmail.com'
    self.app = app
    self.logger = my_logger
    self.solr_url = "https://erc.europa.eu/admin/cordis/solr/reindex"
    self.super_forced = False

    self.cordis_project_result_page = """https://cordis.europa.eu/search/result_en?q='*' AND contenttype='project'"""
    self.cordis_project_page_FP7 =self.cordis_project_result_page + """ AND (programme/code='FP7-IDEAS-ERC')""" #  code or pga , what to choose!
    self.cordis_project_page_H2020 =  self.cordis_project_result_page + """ AND (programme/code='H2020-EU.1.1.*')""" #  code or pga , what to choose!


    self.cordis_scan_FP7_H2020 = self.cordis_project_result_page + """ AND (programme/code='FP7-IDEAS-ERC' OR programme/code='H2020-EU.1.1.*')"""
    self.cordis_project_url = "https://cordis.europa.eu/project/rcn/%s_en.xml"
    self.default_project_filters = ['FP7','H2020']
    self.default_start_page_number = 1
    # self.default_max_page_number = 50
    self.default_items_per_page = 100 # 50

    self.partial_scan_page_limit = 2
    self.full_scan_page_limit = 50

    self.scan_in_progress=False

    self.scan_stats = SortedDict()

    self.db_connections = database_connections
    self.r = self.db_connections.flock['r']
    self.reindexing_solr = False
    self.force_syncronize_mysql = True

  def initialize_scan_stats(self):
    self.partial_scan_page_limit = 4
    self.scan_stats['status'] = 'Not running'
    self.scan_stats['started at'] = ''
    self.scan_stats['started UNIX'] = ''
    self.scan_stats['scan filter'] = []
    self.scan_stats['scan type'] = "Nothing Happening"
    self.scan_stats['active_filter'] = ""

    self.scan_stats['scan page limit']= self.full_scan_page_limit
    self.scan_stats['scan items limit per page'] = self.default_items_per_page
    self.scan_stats['processing type'] = ""
    self.scan_stats['cordis_current_page'] = self.default_start_page_number
    self.scan_stats['cordis_per_page_position'] = 0
    self.scan_stats['projects processed'] = 0
    self.scan_stats['new projects'] = 0
    self.scan_stats['projects updated'] = 0
    self.scan_stats['projects updated list'] = []
    self.scan_stats['projects failed to import'] = []
    self.scan_stats['projects failed to import counter'] = 0
    self.scan_stats['projects failed to retrieve from CORDIS'] = {}
    self.scan_stats['reindexing_solr'] = self.reindexing_solr

  def load_scan_stats_from_db(self, scan_stats):
    for stat_key in scan_stats.keys():
        self.scan_stats[stat_key] = scan_stats[stat_key]


  def get_projects_per_page(self,project_filter,page_number,max_projects_per_page):
    list_of_projects = []
    scan_url = {
      'FP7+H2020' : lambda: self.cordis_scan_FP7_H2020,
      'FP7'     : lambda: self.cordis_project_page_FP7,
      'H2020'   : lambda: self.cordis_project_page_H2020
    }[project_filter]()
    self.logger.info("#85 Going to retrieve :  %s - page_number: %s" %(scan_url,page_number))
    rq = self.load_url(scan_url  + """&p=%s&num=%s&srt=/project/contentUpdateDate:decreasing&format=%s""" %(page_number , max_projects_per_page, 'xml'))
    if rq == False:
       self.logger.error("failed to retrieve the projects per page %s, what now... HELP." %(page_number))
    projects = xmltodict.parse(rq.content)

    if projects['response']['hits'] == None:
      return list_of_projects

    for project in projects['response']['hits']['hit']:
      list_of_projects.append(project['project']['rcn'])

    self.logger.info("rcn' for : %s" %(scan_url  + """&p=%s&num=%s&srt=/project/contentUpdateDate:decreasing&format=%s""" %(page_number , max_projects_per_page, 'xml')))
    self.logger.info(",".join(list_of_projects))
    return list_of_projects


  async def scan_cordis(self, scan_type = "partial", project_filters=["FP7+H2020"] , resume_scan = False, super_forced = False):
    self.scan_in_progress = True
    self.logger.info("starting of the scanning : %s [%s]" %(project_filters,self.scan_in_progress))
    if super_forced == "SNAFU":
      self.logger.info("Super Forced is enabled!!")
      self.super_forced = True

    if not resume_scan:
      #self.scan_type = ', '.join(project_filters)
      self.scan_stats['scan filter'] = project_filters
      self.scan_stats['scan type'] = scan_type
      self.scan_stats['started at'] = time.strftime("%d/%m/%Y - %H:%M:%S")
      self.scan_stats['started UNIX'] = time.time()

      self.scan_stats['status'] = 'In Progress'

      query_result = self.r.db("cordis_processor").table("cordis_processing_status").insert(self.scan_stats).run()

    else:
      current_page = self.scan_stats['cordis_current_page']

    if self.scan_stats['scan type'] == "partial":
      self.scan_stats['scan page limit'] = self.partial_scan_page_limit
    else:
      self.scan_stats['scan page limit'] = self.full_scan_page_limit

    self.scan_stats['active_filter'] = ''

    total_list_of_projects = []

    self.scan_stats['processing type'] = "%s - %s" %(scan_type ,', '.join(project_filters))

    # processing each parameter , but in the same async method
    self.logger.info("Project Filters : %s" %project_filters)
    for project_filter in project_filters:
      await asyncio.sleep(0.1)
      if project_filter != self.scan_stats['active_filter']:
        # resetting the page number + position:
        # TODO : Add dynamic page number.
        self.scan_stats['cordis_current_page'] = self.default_start_page_number
        self.scan_stats['cordis_per_page_position'] = 0
        current_page = self.scan_stats['cordis_current_page']
        per_page_current_project_position = self.scan_stats['cordis_per_page_position']

        self.scan_stats['active_filter'] = project_filter

      self.logger.info("starting with : %s , page : %s  , limit: %s" %(project_filter, current_page, self.default_items_per_page))

      while self.scan_stats['cordis_current_page'] <= self.scan_stats['scan page limit']:
        await asyncio.sleep(0.5)
        if current_page != self.scan_stats['cordis_current_page']:
          current_page = self.scan_stats['cordis_current_page']
          self.scan_stats['cordis_per_page_position'] = 0


        current_page_list_of_projects = self.get_projects_per_page(project_filter,current_page,self.default_items_per_page)
        total_list_of_projects.extend(current_page_list_of_projects)
        if len(current_page_list_of_projects) == 0:
          self.logger.info("No more projects retrieved, canceling the loop")
          break

        for project_rcn in current_page_list_of_projects :
          if resume_scan and (per_page_current_project_position <= self.scan_stats['cordis_per_page_position']) :
            per_page_current_project_position+=1 # only needed when resuming
            self.scan_stats['projects processed'] +=1
            self.logger.info("... skipping %s ..." %(project_rcn))
            continue

          await asyncio.sleep(2.0)
          #await asyncio.sleep(0.2)
          self.logger.info("launching the pre-processing of project: %s" %(project_rcn))
          self.app['process_one_project'] = self.app.loop.create_task(self.container_process_one_project(project_filter,project_rcn))
          #self.logger.info(self.app['process_one_project'])
        self.scan_stats['cordis_current_page'] +=1
        if resume_scan : resume_scan = False


    self.logger.info("******** S C A N   C O M P L E T I N G ... **********")
    if 'process_one_project' in self.app:
      while not self.app['process_one_project'].done():
        self.logger.info("here : %s" %(self.app['process_one_project'].done()))
        await asyncio.sleep(0.5)
    self.logger.info("Finished at : %s" %(time.strftime("%d/%m/%Y - %H:%M:%S")))

    self.logger.info(self.scan_stats)
    mail_status = '' #self.email_stats(self.scan_stats)
    self.reindex_solr()
    self.logger.info("******** S C A N   C O M P L E T I N G  **********")

    self.scan_in_progress = False
    if  self.super_forced:
       self.super_forced = False
       self.logger.info("Disabling Super Forced Scan.")
      #storing results in :
    log_inserted = self.r.db("cordis_processor").table("cordis_process_log").insert({self.scan_stats['started at']:self.scan_stats}).run()
    self.logger.info(log_inserted)
    #resetting the stats!
    self.db_connections.close_connection_to_db()
    self.initialize_scan_stats()
    self.scan_stats['finished at'] = time.strftime("%d/%m/%Y - %H:%M:%S")
    self.scan_stats['status'] = "Succesful completed"
    self.scan_stats['previous_scan filter'] = self.scan_stats['scan filter']

    reset_scan_status = self.r.db("cordis_processor").table("cordis_processing_status").delete().run()

    return total_list_of_projects

  async def scan_list_of_cordis_projects(self, project_filter, rcn_list):
    self.scan_stats['scan filter'] = 'rcn_list'
    self.scan_stats['scan type'] = "scan_list_of_cordis_projects"
    self.scan_stats['started at'] = time.strftime("%d/%m/%Y - %H:%M:%S")
    self.scan_stats['started UNIX'] = time.time()
    self.scan_stats['processing type'] = "list of RCN project ID's"
    for project_rcn in rcn_list:
      time.sleep(1)
      self.app['process_one_project'] = self.app.loop.create_task(self.container_process_one_project(project_filter,project_rcn))

    self.logger.info("******** **** **********")
    while not self.app['process_one_project'].done():
      self.logger.info("here : %s" %(self.app['process_one_project'].done()))
      await asyncio.sleep(1)
    self.logger.info("[slocp] Finished at : %s" %(time.strftime("%d/%m/%Y - %H:%M:%S")))


    self.scan_in_progress = False

      #storing results in :
    log_inserted = self.r.db("cordis_processor").table("cordis_process_log").insert({self.scan_stats['started at']:self.scan_stats}).run()
    self.logger.info('[slocp]:')
    self.logger.info(log_inserted)
    self.reindex_solr()

    #resetting the stats!
    self.initialize_scan_stats()
    self.scan_stats['finished at'] = time.strftime("%d/%m/%Y - %H:%M:%S")
    self.scan_stats['status'] = "Succesful completed"
    self.scan_stats['previous_scan filter'] = self.scan_stats['scan filter']

    reset_scan_status = self.r.db("cordis_processor").table("cordis_processing_status").delete().run()


  async def container_process_one_project(self, project_filter, cordis_rcn):
    await asyncio.sleep(0.5)
    return self.process_one_project(project_filter, cordis_rcn)



  def process_one_project(self, project_filter, cordis_rcn):
    self.logger.info("going to check %s [%s] ..." %(cordis_rcn , project_filter))
    project_details_raw = self.load_url(self.cordis_project_url %(cordis_rcn))
    if project_details_raw == False:
      self.logger.error("Failed to retrieve project : %s (filter: %s" %(cordis_rcn, project_filter))
      if cordis_rcn not in self.scan_stats['projects failed to retrieve from CORDIS']:
        self.scan_stats['projects failed to retrieve from CORDIS'] = 1
      else:
        self.scan_stats['projects failed to retrieve from CORDIS'] += 1
      return ['500' , str(cordis_rcn)]
    self.logger.info("received dataset is %s long..." %(len(project_details_raw.content)))

    project_details = xmltodict.parse(project_details_raw.content)
    project_details = project_details['project']

    if project_filter == 'single_forced':
      self.logger.info("---------------   project_details raw :    ------------------------")
      self.logger.info(project_details)
      self.logger.info("---------------------------------------------------------")

    prepped_project = {}

    try:
      prepped_project['rcn'] = project_details['rcn']
      prepped_project['id'] = project_details['rcn']
      prepped_project['reference'] = project_details['reference']
      prepped_project['acronym'] = project_details['acronym']
      prepped_project['teaser'] = project_details['teaser']
      prepped_project['startDate'] = project_details['startDate']
      prepped_project['endDate'] = project_details['endDate']
      prepped_project['status'] = project_details['status']
      prepped_project['contentUpdateDate'] = project_details['contentUpdateDate']
      prepped_project['title'] =  project_details['title']
      prepped_project['teaser'] =  project_details['teaser']
      prepped_project['totalCost'] = project_details['totalCost']
      prepped_project['status'] = project_details['status']
      prepped_project['objective'] = project_details['objective']
      prepped_project['language'] = project_details['language']
      #prepped_project['availableLanguages'] = project_details['availableLanguages'] # returns OrderedDict([('@readOnly', 'true'), ('#text', 'en')])
      prepped_project['availableLanguages'] = project_details['language']
      prepped_project['ecMaxContribution'] = project_details['ecMaxContribution']
      if 'startDate' in project_details['contract']:
        prepped_project['contract_startDate'] = project_details['contract']['startDate']
        prepped_project['contract_endDate'] = project_details['contract']['endDate']
      else:
        prepped_project['contract_startDate'] = project_details['startDate']
        prepped_project['contract_endDate'] = project_details['endDate']
      prepped_project['contract_duration'] = project_details['contract']['duration']
      prepped_project['contentCreationDate'] = project_details['contentCreationDate']
      prepped_project['contentUpdateDate'] = project_details['contentUpdateDate']
      prepped_project['lastUpdateDate'] = project_details['lastUpdateDate']
      prepped_project['relations'] = json.dumps(project_details['relations'])
      prepped_project['hostInstitution_name']=''
      prepped_project['hostInstitution_country']=''
      prepped_project['hostInstitution_url']=''
      prepped_project['researcher'] = ""
      prepped_project['call_details'] = ''
      prepped_project['call_year'] = ""
      prepped_project['funding_scheme']=""
      prepped_project['subpanel']=""
      prepped_project['statusDetails']=""


      if 'organization' in  project_details['relations']['associations']:
        if type(project_details['relations']['associations']['organization']) is list:
          for organization in project_details['relations']['associations']['organization']:
            if organization['@type'] == 'hostInstitution':
              prepped_project['hostInstitution_name'] = organization['legalName']
              prepped_project['hostInstitution_country'] = self.convert_country(organization['address']['country'])
              if 'url' in organization['address']:
                prepped_project['hostInstitution_url'] = organization['address']['url']
              else:
                prepped_project['hostInstitution_url'] = ''
              if 'associations' in organization['relations']:
                if 'person' in organization['relations']['associations']:
                  persons = organization['relations']['associations']['person']
                  if type(persons) is list:
                    for person in persons:
                      if person['@type'] == "principalInvestigator":
                        prepped_project['researcher'] = """%s %s""" %(person['firstNames'],person['lastName'])
                  elif persons['@type'] == "principalInvestigator":
                    prepped_project['researcher'] = """%s %s""" %(persons['firstNames'],persons['lastName'])

            if organization['@type'] == 'coordinator':
              prepped_project['hostInstitution_name'] = organization['legalName']
              prepped_project['hostInstitution_country'] = self.convert_country(organization['address']['country'])
              if 'url' in organization['address']:
                prepped_project['hostInstitution_url'] = organization['address']['url']
              else:
                prepped_project['hostInstitution_url'] = ''
              if 'associations' in organization['relations']:
                if 'person' in organization['relations']['associations']:
                  persons = organization['relations']['associations']['person']
                  if type(persons) is list:
                    for person in persons:
                      if person['@type'] == "principalInvestigator":
                        prepped_project['researcher'] = """%s %s""" %(person['firstNames'],person['lastName'])
                  elif persons['@type'] == "principalInvestigator":
                    prepped_project['researcher'] = """%s %s""" %(persons['firstNames'],persons['lastName'])


        elif project_details['relations']['associations']['organization']['@type'] == 'hostInstitution' or \
             project_details['relations']['associations']['organization']['@type'] == 'coordinator' :
          organization = project_details['relations']['associations']['organization']
          prepped_project['hostInstitution_name'] = organization['legalName']
          prepped_project['hostInstitution_country'] = self.convert_country(organization['address']['country'])
          if 'url' in organization['address']:
            prepped_project['hostInstitution_url'] = organization['address']['url']
          else:
            prepped_project['hostInstitution_url'] = ''
          if 'associations' in organization['relations']:
            if 'person' in organization['relations']['associations']:
              persons = organization['relations']['associations']['person']
              if type(persons) is list:
                for person in persons:
                  if person['@type'] == "principalInvestigator":
                    prepped_project['researcher'] = """%s %s""" %(person['firstNames'],person['lastName'])
              elif persons['@type'] == "principalInvestigator":
                prepped_project['researcher'] = """%s %s""" %(persons['firstNames'],persons['lastName'])


      if 'call' in  project_details['relations']['associations']:
        if project_details['relations']['associations']['call']['@type'] == 'relatedCall':
          prepped_project['call_details'] = project_details['relations']['associations']['call']['title']
          call_details_info = project_details['relations']['associations']['call']['identifier']
          if call_details_info.find("H2020-") != -1 :
            call_details_info = call_details_info.replace('H2020-','')

          prepped_project['call_year'] = re.search("([1-3][0-9]{3})" , call_details_info)
          if prepped_project['call_year'] != None:
            prepped_project['call_year'] = prepped_project['call_year'].group(0)
            # strange replace for the dates
            if prepped_project['call_year'] == 2020 :
              prepped_project['call_year'] = 2014
          else:
            self.logger.info("call year NOT found in the ['relations']['associations']['call']['identifier'] : %s" %(project_details['relations']['associations']['call']['identifier']))
            self.logger.info("using start year : %s" %(prepped_project['contract_startDate'][0:4]))
            prepped_project['call_year'] = prepped_project['contract_startDate'][0:4]

      category = project_details['relations']['categories']['category']
      if category['@classification'] == 'projectFundingSchemeCategory':
        if '-' in category['code']:
          current_category = category['code'].split('-')[1]
        else:
          current_category = category['code']

        approved_projects= { 'SG'       : 'Starting Grant (StG)',
                             'STG'      : 'Starting Grant (StG)',
                             'CG'       : 'Consolidator Grant (CoG)',
                             'COG'      : 'Consolidator Grant (CoG)',
                             'AG'       : 'Advanced Grant (AdG)',
                             'ADG'      : 'Advanced Grant (AdG)',
                             'POC'      : 'Proof of Concept (PoC)',
                             'SA(POC)'  : 'Proof of Concept (PoC)',
                             'SYG'      : 'Synergy Grants (SyG)',
                             'SA'       : 'Support Actions (SA)',
                             'CSA'      : 'Support Actions (SA)'
                           }

        if current_category.upper() in approved_projects :
          prepped_project['funding_scheme'] = approved_projects[current_category.upper()]

        prepped_project['subpanel'] = ''
        for programme in project_details['relations']['associations']['programme']:
          if programme['@type'] == 'relatedSubProgramme':
            if 'code' in programme:
              prepped_project['subpanel'] = programme['code'].split('-').pop(-1)



      if 'statusDetails' in project_details.keys() : prepped_project['statusDetails'] = project_details['statusDetails']

      project_status, hashed_project = self.check_structure_of_project(prepped_project)
      self.logger.info("[rethinkDB] For Project %s the status was '%s' ." %(project_details['rcn'],project_status))

      if project_filter == 'single_forced':

        self.logger.info("[SINGLE FORCED] %s - RCN : %s - deleting it from mysql if it exists and RE-inserting it into the cordis_projects table" %(project_status , project_details['rcn']) )
        project_status = 'single project forced' #not needed, just keeping it for memory's sake
        mysql_operation = self.delete_project_from_maria_db(prepped_project['rcn'])
        self.logger.info("mysql operation after deleting: %s" %(mysql_operation))

      # if self.project_exists(prepped_project['rcn']):
      self.logger.info(project_status)

      if project_status != "unchanged":
        mysql_operation_successful = False
        try:
          while type(mysql_operation_successful) != int:
            if project_filter == 'single_forced':
              self.logger.info("------  %s -    -------------------- #395" %(project_status))
              self.logger.info(prepped_project)
              self.logger.info("---------------------------------------------------------")

            if project_status == "changed" :
              mysql_operation_successful = self.update_project_in_maria_db(prepped_project)
            elif project_status == "insert as new" or project_filter == 'single_forced' or self.super_forced == True:
              query = """SELECT rcn FROM `ercv2`.`cordis_projects` WHERE `rcn` =  %s;""" %(project_details['rcn'])
              project_exists = self.db_connections.mariadb_execute(query = query)
              if project_exists == 1:
                self.logger.info("project exists ? %s - UPDATING instead of inserting *** SLO" %(project_exists))
                mysql_operation_successful = self.update_project_in_maria_db(prepped_project)
              else:
                mysql_operation_successful = self.insert_project_into_maria_db(prepped_project)
            else:
                mysql_operation_successful = 0
                self.logger.info("no mysql operation needed for %s [%s]" %(project_details['rcn'],project_status))


            # check if mysql operation went ok:
            if type(mysql_operation_successful) != int:
              self.logger.info("Something went WRONG with : %s , going to keep trying..." %(project_details['rcn']) )
              time.sleep(3)
        except Exception as e:
           self.logger.error("#443: issue with MySQL")
           self.logger.info(mysql_operation_successful)
           self.logger.error(e)

        if project_status == "changed" or project_status == 'single project forced':
          self.scan_stats['projects updated'] += 1
          self.scan_stats['projects updated list'].append(project_details['rcn'])
          self.logger.info("updated   %s , with new version [%s] " %(project_details['rcn'],mysql_operation_successful))
        elif project_status == "insert as new":
          self.logger.info("%s :  %s ! " %(project_status , project_details['rcn']))
          if 'new projects' in self.scan_stats:
            self.scan_stats['new projects'] += 1
          else:
            self.logger.warning("The key 'new projects' not in found in stats")
            self.logger.warning(self.scan_stats['new projects'])
      elif self.force_syncronize_mysql:
        self.logger.info("FORCING to CHECK MySQL for PROJECT : %s" %(project_details['rcn']))
        query = """SELECT rcn FROM `ercv2`.`cordis_projects` WHERE `rcn` =  %s;""" %(project_details['rcn'])
        project_exists = self.db_connections.mariadb_execute(query = query)
        if project_exists == 1:
          self.logger.info("project exists ? UPDATING instead of inserting!")
          mysql_operation_successful = self.update_project_in_maria_db(prepped_project)
        else:
          self.logger.info("project Does not Exist - inserting the MISSING project *** %s ***" %(project_details['rcn']))
          mysql_operation_successful = self.insert_project_into_maria_db(prepped_project)

    except Exception as e:
      self.logger.warn("failed to process %s - %s " %(project_details['rcn'], e))
      self.logger.warn("---------------------")
      self.logger.warn(project_details)
      self.logger.warn("---------------------")

      self.scan_stats['projects failed to import counter'] += 1
      self.scan_stats['projects failed to import'].append(project_details['rcn'])

      self.scan_stats['projects processed'] += 1
      self.scan_stats['cordis_per_page_position'] += 1

      self.r.db("cordis_processor").table("cordis_processing_status").filter({"started at": self.scan_stats['started at']}).replace(self.scan_stats).run()
      return ['500' , e]

    # Only add , when mysql went ok, otherwise retry!!!
    self.store_structure_and_hash_of_project(prepped_project,hashed_project,project_status)
    self.scan_stats['projects processed'] += 1
    self.scan_stats['cordis_per_page_position'] += 1

    self.r.db("cordis_processor").table("cordis_processing_status").filter({"started at": self.scan_stats['started at']}).replace(self.scan_stats).run()
    return ["200" , "OK"]

  # def process_all_projects_with_label(self, skip_existing='', project_label='' , cordis_project_page='',
                                      # page_start_number = self.default_start_page_number,
                                      # max_page_nummber = self.default_max_page_number,
                                      # results_per_page = self.default_items_per_page):

    # pass

  def check_structure_of_project(self,prepped_project):
    sorted_prepped_project = self._rebuild_sort_project(prepped_project)
    sorted_prepped_project_json = json.dumps(sorted_prepped_project)

    #hashed_project = str(hash(frozen_project))
    encoded_hashed_project = hashlib.sha256(sorted_prepped_project_json.encode()).hexdigest()
    status = "idle"

    #r.db("test").table("projects").withFields('rcn','acronym','teaser','contentUpdateDate').orderBy('contentUpdateDate').limit(400)
     #check if hash exists, if not it is a new prioject
    if self.r.db("cordis_processor").table("cordis_projects_hashes").filter({'rcn':prepped_project["rcn"]}).count().run() == 0:
      status = "insert as new"
    else:
      #has already exists, we gonna compare it first
      stored_project_hash = self.r.db("cordis_processor").table("cordis_projects_hashes").filter({'rcn':prepped_project["rcn"]}).pluck("project_hash").run()
      stored_project_hash = stored_project_hash.items[0]['project_hash']
      # encoded_stored_project_hash = hashlib.sha256(stored_project_hash.encode()).hexdigest()
      #self.logger.info(encoded_hashed_project)
      #self.logger.info(stored_project_hash)
      if stored_project_hash != encoded_hashed_project:
        status = "changed"
      else:
        status = "unchanged"
        #no change, not to do
    #if self.r.db("cordis_processor").table("cordis_projects_hashes").filter({'rcn':prepped_project["rcn"]}).count().run() == 0:
#     self.r.db("cordis_processor").table("cordis_projects_hashes").insert({"rcn" : prepped_project["rcn"] , "project_hash":hashed_project})
#     self.r.db("cordis_processor").table("cordis_projects_hashes").insert({"rcn" : "1234","project_hash":"12348746545"})
#     self.logger.info(prepped_project)
    return status,encoded_hashed_project

  def store_structure_and_hash_of_project(self,prepped_project,hashed_project,status):

      if status == "insert as new" :
        self.r.db("cordis_processor").table("cordis_projects_hashes").insert({"rcn" : prepped_project["rcn"] , "project_hash":hashed_project}).run()
        # storing local version
        self.r.db("cordis_processor").table("cordis_projects").insert(prepped_project).run()
        self.logger.info("inserted as new with hash : %s" %(hashed_project))
      elif status == "changed" :
        #means a changes has occured , will update the stored hash with the newly generated one
        self.r.db("cordis_processor").table("cordis_projects_hashes").filter({'rcn':prepped_project["rcn"]}).update({"project_hash":hashed_project}).run()
        # also storing the differences
        stored_project = self.r.db("cordis_processor").table("cordis_projects").get(prepped_project["rcn"]).run()
        project_diff =DeepDiff(prepped_project,stored_project)

        if project_diff.__len__() > 0 :
          self.logger.info("storing project diff in 'cordis_projects_update_history' ...")
          new_update_history = {'runtime_unix': self.scan_stats['started UNIX'] ,'runtime': self.scan_stats['started at'] , 'rcn':prepped_project["rcn"] , 'data':project_diff}
          try:
            self.r.db("cordis_processor").table("cordis_projects_update_history").insert(new_update_history).run()
          except Exception as e:
            self.logger.error("In 'store_structure_and_hash_of_project':")
            self.logger.error(e)
        # update the stored version
        self.r.db("cordis_processor").table("cordis_projects").get(prepped_project["rcn"]).replace(prepped_project).run()
    # try:
    # except Exception as e:
      # self.logger.error("In 'store_structure_and_hash_of_project':")
      # self.logger.error(e)
      # exc_type, ex, tb = sys.exc_info()
      # f = tb.tb_frame
      # filename = f.f_code.co_filename
      # imported_tb_info = traceback.extract_tb(tb)[-1]
      # line_number = imported_tb_info[1]
      # print_format = 'Filename: {} - {}: Exception in line: {}, message: {}'
      # self.logger.error(print_format.format(filename, exc_type.__name__, line_number, ex))
### TODO  fix error:
    #type object 'str' has no attribute '__code__'
    #/opt/cpd/cordisProjectProcessor.py [16579]: ERROR AttributeError: Exception in line: 1828, message: type object 'str' has no attribute '__code__

  def convert_country(self, country_iso):
    full_country_name = ""
    if country_iso != "":
      strange_country_name = {"EL": "GR" , "UK":"GB"}
      if country_iso in strange_country_name.keys():
        country_iso = strange_country_name[country_iso]
      try:
        full_country_name = pycountry.countries.lookup(country_iso).name
      except Exception as e:
        self.logger.error("country iso code not found , %s" %(country_iso))
        pass

    return full_country_name

  def project_exists(self,cordis_rcn):
    query = """SELECT `rcn` FROM `ercv2`.`cordis_projects` WHERE `rcn` = %s"""
    result = self.db_connections.mariadb_execute(query=query,values=[cordis_rcn])

    if result == 1:
      return True
    else:
      return False

  def insert_project_into_maria_db(self,project):
    query = """INSERT INTO `ercv2`.`cordis_projects` (`rcn`,
                                                      `id`,
                                                      `acronym`,
                                                      `researcher`,
                                                      `startdate`,
                                                      `enddate`,
                                                      `reference`,
                                                      `status`,
                                                      `title`,
                                                      `teaser`,
                                                      `objective`,
                                                      `language`,
                                                      `availableLanguages`,
                                                      `ecMaxContribution`,
                                                      `totalCost`,
                                                      `contract_startDate`,
                                                      `contract_endDate`,
                                                      `contract_duration`,
                                                      `statusDetails`,
                                                      `contentCreationDate`,
                                                      `contentUpdateDate`,
                                                      `lastUpdateDate`,
                                                      `relations`,
                                                      `hostInstitution_name`,
                                                      `hostInstitution_country`,
                                                      `hostInstitution_url`,
                                                      `call_details`,
                                                      `call_year`,
                                                      `funding_scheme`,
                                                      `subpanel` ) VALUES (%s,
                                                                           %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s,
                                                                               %s);"""

    result = self.db_connections.mariadb_execute(query = query,values = (project['rcn'],
                              project['id'],
                              project['acronym'],
                              project['researcher'],
                              project['startDate'],
                              project['endDate'],
                              project['reference'],
                              project['status'],
                              project['title'],
                              project['teaser'],
                              project['objective'],
                              project['language'],
                              project['availableLanguages'],
                              project['ecMaxContribution'],
                              project['totalCost'],
                              project['contract_startDate'],
                              project['contract_endDate'],
                              project['contract_duration'],
                              project['statusDetails'],
                              project['contentCreationDate'],
                              project['contentUpdateDate'],
                              project['lastUpdateDate'],
                              project['relations'],
                              project['hostInstitution_name'],
                              project['hostInstitution_country'],
                              project['hostInstitution_url'],
                              project['call_details'],
                              project['call_year'],
                              project['funding_scheme'],
                              project['subpanel']
                              ))
    return result

  def update_project_in_maria_db(self,project):

    query = """UPDATE `ercv2`.`cordis_projects` SET  `acronym` =  %s,
                                                     `researcher` =  %s,
                                                     `startdate` =  %s,
                                                     `enddate` =  %s,
                                                     `reference` =  %s,
                                                     `status` =  %s,
                                                     `title` =  %s,
                                                     `teaser` =  %s,
                                                     `objective` =  %s,
                                                     `language` =  %s,
                                                     `availableLanguages` =  %s,
                                                     `ecMaxContribution` =  %s,
                                                     `totalCost` =  %s,
                                                     `contract_startDate` =  %s,
                                                     `contract_endDate` =  %s,
                                                     `contract_duration` =  %s,
                                                     `statusDetails` =  %s,
                                                     `contentCreationDate` =  %s,
                                                     `contentUpdateDate` =  %s,
                                                     `lastUpdateDate` =  %s,
                                                     `relations` =  %s,
                                                     `hostInstitution_name` =  %s,
                                                     `hostInstitution_country` =  %s,
                                                     `hostInstitution_url` =  %s,
                                                     `call_details` =  %s,
                                                     `call_year` =  %s,
                                                     `funding_scheme` =  %s,
                                                     `subpanel`  =  %s
                                               WHERE `rcn` =  %s
                                                   ;"""

    result = self.db_connections.mariadb_execute(query = query,values = (project['acronym'],
                                                                       project['researcher'],
                                                                       project['startDate'],
                                                                       project['endDate'],
                                                                       project['reference'],
                                                                       project['status'],
                                                                       project['title'],
                                                                       project['teaser'],
                                                                       project['objective'],
                                                                       project['language'],
                                                                       project['availableLanguages'],
                                                                       project['ecMaxContribution'],
                                                                       project['totalCost'],
                                                                       project['contract_startDate'],
                                                                       project['contract_endDate'],
                                                                       project['contract_duration'],
                                                                       project['statusDetails'],
                                                                       project['contentCreationDate'],
                                                                       project['contentUpdateDate'],
                                                                       project['lastUpdateDate'],
                                                                       project['relations'],
                                                                       project['hostInstitution_name'],
                                                                       project['hostInstitution_country'],
                                                                       project['hostInstitution_url'],
                                                                       project['call_details'],
                                                                       project['call_year'],
                                                                       project['funding_scheme'],
                                                                       project['subpanel'],
                                                                       project['rcn'],
                                                                       ))
    return result

  def delete_project_from_maria_db(self,project_rcn):
    query = """DELETE FROM `ercv2`.`cordis_projects` WHERE `rcn` =  %s;""" %(project_rcn)
    result = self.db_connections.mariadb_execute(query = query)

    return result

  def email_stats(self, stats):
   try:
     current_time = time.strftime("%d/%m/%Y - %H:%M:%S")
     text =json.dumps(stats)
     # msg = MIMEText(text) # limited in Size , maximum 998 Octets
     msg = MIMEMultipart()
     msg.attach(MIMEText(text, "plain"))
     msg['From'] = self.say_my_name
     msg['To'] = self.email_for_notifications
     msg['Subject'] = "CPD processor stats,a %s scan of %s started on: %s and ended at :%s " %(self.scan_stats['scan type'],self.scan_stats['processing type'],stats['started at'],current_time)

     msg = msg.as_string()
     eserver = smtplib.SMTP('localhost')
     #s.send_message(msg)
     eserver.sendmail(self.say_my_name, self.email_for_notifications, msg)
     self.logger.info(eserver)
     eserver.quit()
     self.logger.info("Mailed log to the Watcher!")
   except Exception as e:
     self.logger.error("sending the eMail went wrong:")
     self.logger.error(e)

   return True

  def reindex_solr(self):
    self.logger.info('[SOLR] indexing : %s' %(self.reindexing_solr))
    if self.reindexing_solr == False:
      if self.scan_stats['projects updated'] > 0 or self.scan_stats['new projects'] > 0 or self.super_forced:
        self.logger.info('[SOLR] going to reindex Solr...')
        self.reindexing_solr = True

        self.app.loop.create_task(self.pew())
      else:
        self.logger.info('[SOLR] Nothing Changed, canceling indexing!')
    else:
      self.logger.warning('[SOLR] - Already reindexing at the moment, RUHO.')


  async def pew(self):
    # self.load_url(self.solr_url , 60)
    #loop = asyncio.get_event_loop()
    #p = ProcessPoolExecutor(2)
    #yield from loop.run_in_executor(p, load_url, self.solr_url )
   self.logger.info("SOLR] Triggering SOLR job...")
   response = self.load_url(self.solr_url,180 )
   self.logger.info(response)
   self.reindexing_solr = False
   self.scan_stats['SOLR Last updated at'] = time.strftime("%d/%m/%Y - %H:%M:%S")
   #with ProcessPoolExecutor(max_workers=1) as executor:
   #  job = executor.submit(self.load_url,self.solr_url )
   #  while not job.done():
   #    self.logger.info("SOLR job is not done yet...")
   #    await asyncio.sleep(0.8)
   #    job.cancel()
   #  self.logger.info("after le job is done from , which is now, await")
   #  self.reindexing_solr=False
   #  return True

     #unsent_request = [grequests.get(self.solr_url, hooks={'response': self.my_async_response_handler} , exception_handler=self.my_async_exception_handler,  timeout=60)]
     #send_rq = grequests.map(unsent_request)
     #or send_rq = unsent_request[0].send()
     #send_rq = unsent_request[0].send()

    # result = xmltodict.parse(rq.content)

    #URLS = [self.solr_url]
    # We can use a with statement to ensure threads are cleaned up promptly
    #with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Start the load operations and mark each future with its URL
        # future_to_url = {executor.submit(self.load_url, url, 60): url for url in URLS}
        # for future in concurrent.futures.as_completed(future_to_url):
        #     url = future_to_url[future]
        #     try:
        #         data = future.result()
        #     except Exception as exc:
        #         self.logger.error('%r generated an exception: %s' % (url, exc))
        #     else:
        #         self.logger.info('%r page is %d bytes' % (url, len(data)))
        #         self.logger.info(data)


  # Retrieve a single page and report the URL and contents
  def load_url(self,url,my_timeout=60):
    #unsent_request = grequests.get(url, hooks={'response': self.my_async_response_handler}, timeout=timeout)
    #sent_request = unsent_request.send();
    #url_response = sent_request.response.content
    url_response = False
    t0 = time.time()
    try:
      #self.logger.info("OUT OF TIME in %s seconds." %(my_timeout))
      url_response = self.requests_retry_session().get(url,timeout=my_timeout)
      #content = url_response.content
    except Exception as e:
      self.logger.error('Issue retrieving the url %s ( %s ) :' %(url,url_response))#x.__class__.__name__))
      self.logger.error(url_response)
      self.logger.error(e)
    else:
      self.logger.info('Successful retrieval for :%s - status_code: %s' %(url, url_response.status_code))
    finally:
      t1 = time.time()
      self.logger.info('Took %s seconds' %(t1 - t0))
    return url_response

  def requests_retry_session(
    total=11,
    retries=3,
    backoff_factor=0.4,
    status_forcelist=(500, 502, 504),
    session=None,
    ):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


  def my_async_response_handler(self, response, *args, **kwargs):
    self.logger.info('inside the magic box')
    self.reindexing_solr=False
    # self.logger.info(response.content)

  def my_async_exception_handler(self,request, exception):
    self.logger.error('Request failed: %' %(exception))


#### CAN'T USE FREEZE , because no guarantee of the value hashed data  of an frozenset object
#### the hash is the same during the process, but restarting the daemon, then the frozenset generate different hashes
  def __crap_freeze(self,o):
    if isinstance(o,SortedDict):
      new_dict = { k:self._freeze(v) for k,v in o.items()}
      new_dict = SortedDict(sorted(new_dict.items(), key=lambda x: x[1]))
      return frozenset(new_dict)
    if isinstance(o,dict):
      # converting to  SortedDict , otherwise the keys are not always in the same order (which is crucial for the hash comparison)
      sorted_items = SortedDict(sorted(o.items(), key=lambda x: x[1]))
      new_dict = { k:self._freeze(v) for k,v in sorted_items.items()}
      new_dict = SortedDict(sorted(new_dict.items(), key=lambda x: x[1]))
      return frozenset(new_dict)
    if isinstance(o,list):
      return tuple([self._freeze(v) for v in o])
    return o


#### CAN'T USE FREEZE , because no guarantee of the value hashed data  of an frozenset object
#### the hash is the same during the process, but restarting the daemon, then the frozenset generate different hashes
  def _freeze(self, o):
    if isinstance(o,SortedDict):
      return frozenset({ k:self._freeze(v) for k,v in o.items()}.items())
    if isinstance(o,dict):
      return frozenset({ k:self._freeze(v) for k,v in o.items()}.items())
    if isinstance(o,list):
      return tuple([self._freeze(v) for v in o])
    return o

  def _rebuild_sort_project(self, o):
    if isinstance(o,dict) or isinstance(o,SortedDict):
          new_dict=SortedDict()
          sorted_keys = sorted(o.keys())

          for sorted_key in sorted_keys:
            if isinstance(o[sorted_key],dict):
                  new_dict[sorted_key] = self._rebuild_sort_project(o[sorted_key])
            else:
              new_dict[sorted_key] = o[sorted_key]
    return  new_dict

def Bad_load_url(url,timeout=60):
     #unsent_request = grequests.get(url, hooks={'response': self.my_async_response_handler}, timeout=timeout)
     #sent_request = unsent_request.send();
     #url_response = sent_request.response.content
     url_response = requests.get(url,timeout=timeout)
     return url_response


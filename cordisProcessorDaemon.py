#!/usr/bin/env python3
#from pudb import set_trace; set_trace()
import base64 , json , re, time , sys, traceback
# import MySQLdb
from aiohttp import web
import asyncio
import requests
import xmltodict
import rethinkdb
from sortedcontainers import SortedDict

import dbConnections
import cordisProjectProcessor

from pprint import pprint

import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler #RotatingFileHandler

class handler_cordis_api:
    def __init__(self,app):
        self.app = app
        self.last_run_on_human_friendly = time.strftime("%d/%m/%Y , %H:%M:%S")
        self.last_run_on = time.time()
        self.logger = self.set_up_logging()

        self.cordis_projects = ['h2020', 'other']
        self.max_retries_per_project = 5
        self.db_connections = dbConnections.DbConnections(self.logger)
        self.db_connections.connect_to_db("mariadb")
        self.db_connections.connect_to_rethink()
        self.cordisProject = cordisProjectProcessor.CordisProject(self.db_connections, self.app,self.logger)

        self.init_rethinkdb()
        # self.db_connections.connect()

    def set_up_logging(self):
      serverlog_path ="/var/log/cordis_processor"
      # File handler for /var/log/some.log
      #serverlog = logging.FileHandler('%s_%s.log' %(serverlog_path,time.strftime("%Y-%m")))
      #serverlog.setLevel(logging.DEBUG)
      #serverlog.setFormatter(logging.Formatter('%(asctime)s %(pathname)s [%(process)d]: %(levelname)s %(message)s'))

      # Combined logger used elsewhere in the script
      logger = logging.getLogger(__name__)
      logger.setLevel(logging.DEBUG)
      #logger.addHandler(serverlog)

      # add a rotating handler
      #handler = RotatingFileHandler(path, maxBytes=10000000, backupCount=10)
      handler = TimedRotatingFileHandler("%s.log" %serverlog_path,
                                         when="d",
                                         interval=1,
                                         backupCount=10)
      handler.setFormatter(logging.Formatter('%(asctime)s %(pathname)s [%(process)d]: %(levelname)s %(message)s'))
      logger.addHandler(handler)

      logger.info('Started logging at %s' %(self.last_run_on_human_friendly))
      return logger

    def init_rethinkdb(self):
      r = self.db_connections.flock['r']
      if "cordis_processor" not in r.db_list().run():
        r.db_create("cordis_processor").run()
      if "stats" not in r.db("cordis_processor").table_list().run():
        r.db("cordis_processor").table_create("stats").run()
      if "cordis_projects" not in r.db("cordis_processor").table_list().run():
        r.db("cordis_processor").table_create("cordis_projects").run()
      if "cordis_projects_hashes" not in r.db("cordis_processor").table_list().run():
        r.db("cordis_processor").table_create("cordis_projects_hashes").run()

      if "cordis_process_log" not in r.db("cordis_processor").table_list().run():
        r.db("cordis_processor").table_create("cordis_process_log").run()

      #  r.db("test").table_drop("projects").run()
      # storing the differences per run, if there are differences
      if "cordis_projects_update_history" not in r.db("cordis_processor").table_list().run():
        r.db("cordis_processor").table_create("cordis_projects_update_history").run()
      # containing data set , when running a full / partial update , so after a restart we can continue where we left off?
      if "cordis_processing_status" not in r.db("cordis_processor").table_list().run():
        r.db("cordis_processor").table_create("cordis_processing_status").run()
      else:
        #Check if there was a scan in progress:
        self.logger.info("Check if there was a scan in progress:")
        scan_status = r.db("cordis_processor").table("cordis_processing_status").run()
        scan_status = scan_status.items
        if len(scan_status) > 0 and 'status' in scan_status[0]:
          # there was a scan in progress, status is paused?
          self.logger.info("there was a scan in progress!! : %s" %(scan_status[0]['scan type']))
          self.cordisProject.scan_in_progress = True
          #self.cordisProject.scan_type = scan_status[0]['scan type']
          self.cordisProject.load_scan_stats_from_db(scan_status[0])
        else:
          self.cordisProject.initialize_scan_stats()
          self.logger.info('no scan was in progress')
        self.logger.info(scan_status)

    async def process_one_cordis_project(self, request):
      project_rcn = request.match_info.get('rcn',"no rcn included")
      # self.app.loop.create_task(self.cordisProject.process_one_project(project_rcn))
      process_status = "FAILED"
      try:
        #app['scan_partial'] = app.loop.create_task(self.cordisProject.process_one_project(project_rcn))
        process_status = self.cordisProject.process_one_project('single_forced',project_rcn)
        json = {'response': "Project {} processed {}".format(project_rcn, process_status) , 'status':'200'}
      except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()

        traceback_details = {
                             'filename': exc_traceback.tb_frame.f_code.co_filename,
                             'lineno'  : exc_traceback.tb_lineno,
                             'name'    : exc_traceback.tb_frame.f_code.co_name,
                             'type'    : exc_type.__name__,
                             'message' : exc_value.message, # or see traceback._some_str()
                            }

        process_status +=  "%s" %(e)

        json = {'response': "Project {} failed {}".format(project_rcn, process_status) ,
                'traceback_details' : traceback_details ,
                'status':'500'}

      self.cordisProject.reindex_solr()
      return web.json_response(json)

    async def process_list_of_cordis_projects(self, request):
      project_rcns = request.match_info.get('rcns',False)
      # self.app.loop.create_task(self.cordisProject.process_one_project(project_rcn))
      process_status = "FAILED"
      self.cordisProject.scan_stats['scan type'] = "importing set of RCN's"
      try:
        rcn_list = project_rcns.split(',')
        if self.cordisProject.scan_in_progress == False:
          self.cordisProject.scan_in_progress = True
          app['scan_cordis'] = app.loop.create_task(self.cordisProject.scan_list_of_cordis_projects('single_forced',rcn_list))
          self.cordisProject.scan_stats['status'] = 'Running'
          json = {'response': "Project {} processed {}".format(rcn_list, self.cordisProject.scan_stats['status']) , 'status':'200'}
        else:
          json = {'response': self.scan_stats(), 'status':'200'}

      except Exception as e:
        process_status +=  " %s" %(e)
        json = {'response': "Project {} failed {}".format(rcn_list, self.cordisProject.scan_stats['status']) , 'status':'500'}

      return web.json_response(json)

    def handle_last_updated(self):
        return self.last_run_on_human_friendly

    def handle_update_set_of_projects(self, data):
        try:
            while (len(data) > 0):
              # take last item in te list
              current_project_imported = False
              current_project_retry_amount = 0
              current_project = data[-1]


              #import it successfully
              if current_project_imported is true:
                data.pop()
              elif current_project_retry_amount > 5:
                 data.pop()
                 #TODO : log this project
              else:
                current_project_retry_amount += 1


        except async.CancelledError:
            pass
        finally:
            pass

        pass

    def full_scan_of_cordis(self):
        scanning_finished = Fale
        try:
            while (not scanning_finished):
                break
        except async.CancelledError:
            pass
        finally:
            pass

        pass

    # def projces

    def partial_scan_of_cordis(self, request):
      process_status = ""
      custom_limit = request.match_info.get('limit',False)
      if custom_limit != False:
        self.cordisProject.partial_scan_page_limit = int(custom_limit)
      self.logger.info("Is there a scan in progress ? %s - new limit : %s"  %(self.cordisProject.scan_in_progress , custom_limit))
      try:
        if self.cordisProject.scan_in_progress == False:
          app['scan_cordis'] = app.loop.create_task(self.cordisProject.scan_cordis('partial'))
          self.cordisProject.scan_stats['status'] = 'Running'
          #process_status = self.cordisProject.scan_partial()
       # elif request.match_info.get('resume', "") == 'resume':
       #   self.logger.info("big scan in progress : %s and will resume!"  %self.cordisProject.scan_in_progress)
       #   app['scan_cordis'] = app.loop.create_task(self.cordisProject.scan_cordis(['partial'],True))
        else:
          process_status = self.scan_stats()
      except Exception as e:
        process_status =  "%s" %(e)


      txt = "Hello there ... 200"

      json_output= {'response': "{}".format(txt), 'process status' : process_status }
      return web.json_response(json_output)

    def full_scan_of_cordis(self, request):
      # STOP THE CURRENT SCANNING
      if self.cordisProject.scan_in_progress == True:
        #app['scan_cordis'].cancel()
        if 'scan_cordis' in app:
          scan_cordis = app['scan_cordis']
          scan_cordis.cancel()
          process_one_project = app['process_one_project']
          process_one_project.cancel()
          txt = "canceled current scan : %s" %(self.cordisProject.scan_stats['scan type'])
        else:
          txt = "canceled pending scan, not running at the moment"

        self.cordisProject.scan_in_progress = False
        self.logger.info(txt)

        r = self.db_connections.flock['r']
        self.cordisProject.scan_stats['finished at'] = time.strftime("%d/%m/%Y - %H:%M:%S")
        self.cordisProject.scan_stats['started UNIX'] = time.time()

        self.cordisProject.scan_stats['status']  = "Manually stopped"
        store_previous_run = r.db("cordis_processor").table("cordis_process_log").insert(self.cordisProject.scan_stats).run()
        reset_scan_status = r.db("cordis_processor").table("cordis_processing_status").delete().run()
        self.cordisProject.initialize_scan_stats()

      process_status = ""
      default_project_filters = self.cordisProject.default_project_filters

      offset_page = request.match_info.get('offset_page',False)
      custom_project_filter = request.match_info.get('project_filter',False)

      if custom_project_filter in default_project_filters:
        project_filter = custom_project_filter.split(',')
        self.cordisProject.scan_stats['scan type'] = project_filter
      else:
       project_filter = default_project_filters

      if offset_page != False:
        self.cordisProject.scan_stats['cordis_current_page'] = int(offset_page)

      super_forced = request.match_info.get('super_forced',False)
      self.logger.info("big scan in progress ? %s -- offset_page : %s -- super_forced : %s"  %(self.cordisProject.scan_in_progress , offset_page, super_forced))
      try:
        if self.cordisProject.scan_in_progress == False:
          app['scan_cordis'] = app.loop.create_task(self.cordisProject.scan_cordis('Full Scan', project_filter, False, super_forced))
          self.cordisProject.scan_stats['status'] = 'Running'
          #process_status = self.cordisProject.scan_partial()
        else:
          process_status = self.scan_stats()
      except Exception as e:
        process_status =  "%s" %(e)

      txt = "Hello there , started full scan of  %s starting from page %s . The processing has begun!" %(project_filter , self.cordisProject.scan_stats['cordis_current_page'] )

      json_output= {'response': "{}".format(txt), 'process status' : process_status , 'status':200}
      return web.json_response(json_output)

    def scan_stats(self):
      message  = """there is already a scan in progress : %s""" %(self.cordisProject.scan_stats['scan type'])
      return message

    def process_status(self,request):
      process_status = self.cordisProject.scan_stats
      process_status_json = json.dumps(process_status)
      if self.cordisProject.scan_in_progress == True:
        txt = "Hello, there! Looks Like something is in the oven."
      else:
        txt = "Hello, nothing is happening at the moment.."

      json_output= {'response': "{}".format(txt), 'process status' : process_status }

      return web.json_response(json_output)
      #return web.Response(text=txt)
    def resume_scan_of_cordis(self,request):
      txt='200'
      process_status = self.cordisProject.scan_stats
      process_status_json = json.dumps(process_status)
      if self.cordisProject.scan_in_progress == True:
        if self.cordisProject.scan_stats['status'] == 'Paused':
          self.logger.info("Resuming scan.")
          self.logger.info("Scan pending : %s and will resume!"  %self.cordisProject.scan_in_progress)
          scan_type = self.cordisProject.scan_stats['scan type']
          current_filter = self.cordisProject.scan_stats['scan filter']
          app['scan_cordis'] = app.loop.create_task(self.cordisProject.scan_cordis(scan_type,current_filter,True))
          self.cordisProject.scan_stats['status'] = 'Running'
      else:
        txt = "No scan in progess. "
      json_output= {'response': "{}".format(txt), 'process status' : process_status }
      return web.json_response(json_output)

    def pause_processing(self,request):
      txt = "Nothing to pauze here."
      if self.cordisProject.scan_in_progress == True:
        self.logger.info("Pausing the scan")
        txt='Nothing running at the moment.'

        process_status = self.cordisProject.scan_stats
        process_status_json = json.dumps(process_status)
        if self.cordisProject.scan_in_progress == True:
          #app['scan_cordis'].cancel()
          if 'scan_cordis' in app:
            scan_cordis = app['scan_cordis']
            scan_cordis.cancel()
            self.cordisProject.scan_stats['status'] = 'Paused'
            txt = "canceled current scan : %s" %(self.cordisProject.scan_stats['scan type'])
        else:
          txt = "canceled pending scan, not running at the moment"\

      json_output = {'response': "{}".format(txt)}
      return web.json_response(json_output)

    def stop_processing(self, request):
      process_status = self.cordisProject.scan_stats
      process_status_json = json.dumps(process_status)
      txt = "Going to cancel a scan, if there is one, if not, nothing will happen..."
      if self.cordisProject.scan_in_progress == True:
        #app['scan_cordis'].cancel()
        if 'scan_cordis' in app:
          scan_type = self.cordisProject.scan_stats['scan type']
          scan_cordis = app['scan_cordis']
          scan_cordis.cancel()
          txt = "canceled current scan : %s" %(scan_type)
          if 'process_one_project' in app:
            process_one_project = app['process_one_project']
            process_one_project.cancel()
        else:
          txt = "canceled pending scan, not running at the moment"

        self.cordisProject.scan_in_progress = False
        self.cordisProject.scan_stats['scan type'] = "Resetted"

        r = self.db_connections.flock['r']
        store_previous_run = r.db("cordis_processor").table("cordis_process_log").insert(self.cordisProject.scan_stats).run()
        reset_scan_status = r.db("cordis_processor").table("cordis_processing_status").delete().run()
        self.cordisProject.initialize_scan_stats()

        self.cordisProject.scan_stats['finished at'] = time.strftime("%d/%m/%Y - %H:%M:%S")
        self.cordisProject.scan_stats['started UNIX'] = time.time()

        self.cordisProject.scan_stats['status']  = "Manually stopped"
        self.cordisProject.reindex_solr()
      else:
        txt = "No scan in progress"

      self.logger.info(txt)
      json_output= {'response': "{}".format(txt), 'process status' : process_status }
      return web.json_response(json_output)

    def handle_intro(self, request):
      peername = request.transport.get_extra_info('peername')
      txt = "Hello , Anonymous."
      if peername is not None:
        host, port = peername
        self.logger.info("visited by: %s - %s" %(host,port))
        txt = "Hello there, Mr. %s" %(host)

      return web.Response(text=txt)

    def get_all_logs(self, request):
      all_logs={}
      try:
        r = self.db_connections.flock['r']
        all_logs_raw = r.db("cordis_processor").table("cordis_process_log").order_by(r.desc('started UNIX')).limit(384).run()
        all_logs = list(all_logs_raw)
        json_output= {'response': all_logs, 'status' : 200}
        # self.logger.info( all_logs_raw)
      except Exception as e:
        self.logger.error(e)
        json_output= {'response': e, 'status': 500}

      return web.json_response(json_output)

#class handler_web_api:
#
#    def __init__(self):
#        self.counter=0
#        pass
#
#    def handle_intro(self, request):
#        return web.Response(text="Hello, world")
#
#    async def handle_greeting(self, request):
#        name = request.match_info.get('name', "Anonymous")
#        txt = "Hello, {}".format(name)
#        return web.Response(text=txt)
#
#        handler = Handler()
#    def add_counter(self):
#      self.counter += 1
#      #return web.Response(text="added")
#
#    def read_counter(self, request):
#      return web.Response(text="Hello, world , counted so far: %s" %(self.counter))
#
#    def background_tasks(self,request):
#      app['redis_listener'] = app.loop.create_task(listen_to_redis(app, handler))
#      return web.Response(text="Hello, the background task has been started")
#
#
#    async def cleanup_background_tasks(app):
#        app['redis_listener'].cancel()
#        await app['redis_listener']
#
#
#    async def start_background_tasks(app):
#        app['redis_listener'] = app.loop.create_task(listen_to_redis(app, handler))
#
#    async def listen_to_redis(app, handler):
#        try:
#          while (handler.counter < 100):
#            print("here.")
#            await asyncio.sleep(1)
#            print("test %s" %(handler.counter))
#            handler.add_counter()
#        except asyncio.CancelledError:
#            pass
#        finally:
#            pass


app = web.Application()
#handler = handler_web_api()
handler_cordis = handler_cordis_api(app)
app.router.add_get('/', handler_cordis.handle_intro)
#app.router.add_get('/intro', handler.handle_intro)
#app.router.add_get('/greet/{name}', handler.handle_greeting)
#app.router.add_get('/add', handler.add_counter)
#app.router.add_get('/counted', handler.read_counter)
#app.router.add_get('/bg', handler.background_tasks)
app.router.add_get('/process_one_project/{rcn}', handler_cordis.process_one_cordis_project)
app.router.add_get('/process_list_of_cordis_projects/{rcns}',handler_cordis.process_list_of_cordis_projects)
app.router.add_get('/process_latest_projects', handler_cordis.partial_scan_of_cordis)
app.router.add_get('/process_latest_projects/{limit}', handler_cordis.partial_scan_of_cordis)
app.router.add_get('/process_all_projects', handler_cordis.full_scan_of_cordis)
app.router.add_get('/process_all_projects/{offset_page}', handler_cordis.full_scan_of_cordis)
app.router.add_get('/process_all_projects/{offset_page}/{super_forced}', handler_cordis.full_scan_of_cordis)
app.router.add_get('/process_all_projects_one_filter/{project_filter}/{offset_page}/{super_forced}', handler_cordis.full_scan_of_cordis)
app.router.add_get('/process_status', handler_cordis.process_status)
app.router.add_get('/stop_processing', handler_cordis.stop_processing)
app.router.add_get('/resume', handler_cordis.resume_scan_of_cordis)
app.router.add_get('/pause',  handler_cordis.pause_processing)

app.router.add_get('/get_all_logs', handler_cordis.get_all_logs)





# app.on_startup.append(start_background_tasks)
#app.on_cleanup.append(cleanup_background_tasks)


web.run_app(app, port=5222)

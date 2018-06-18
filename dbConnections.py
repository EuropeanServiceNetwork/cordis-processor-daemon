import MySQLdb
import rethinkdb

class DbConnections:
    def __init__(self, my_logger, config):
      self.logger = my_logger
      self.db_config = config['database']
      self.flock = {}
      self.db_cursors = {}

    def connect_to_db(self,database_name="mariadb"):
      self.flock[database_name] = MySQLdb.connect(user=self.db_config['mariadb_username'],passwd=self.db_config['mariadb_passwd'],db=self.db_config['mariadb_database'], charset='utf8', init_command='SET NAMES UTF8')
      self.flock[database_name].autocommit(True)

    def close_connection_to_db(self, database_name="mariadb"):
      self.db_cursors[database_name].close()
      self.db_cursors.__delitem__(database_name)
      self.flock[database_name].close()
      self.flock.__delitem__(database_name)

    def create_new_cursor_for(self, database_name="mariadb"):
      if database_name in self.db_cursors:
        self.db_cursors[database_name].close()
        self.db_cursors.__delitem__(database_name)

      if database_name not in self.flock:
        self.connect_to_db(database_name)


      self.db_cursors[database_name] = self.flock[database_name].cursor()

    def mariadb_execute(self, database_name = "mariadb", query = "", values = list()):
      query_result = False
      try:
        if database_name not in self.db_cursors:
          self.create_new_cursor_for(database_name)
        elif self.db_cursors[database_name] == None:
          self.create_new_cursor_for(database_name)

        query_result = self.db_cursors[database_name].execute(query,values)

      except (AttributeError, MySQLdb.OperationalError):
        self.logger.error(MySQLdb.OperationalError)
        self.logger.info("closing current cursor and connection, and return an empty resultset")
        self.flock[database_name].close()
        self.flock.__delitem__(database_name)

        self.create_new_cursor_for(database_name)
        # Execute the query again
        query_result = self.db_cursors[database_name].execute(query,values)

      except Exception as e:
        self.logger.error("------- WHOOPS -----")
        self.logger.error(e)

      #self.logger.info("query result : %s" %(query_result) )

      return query_result

    def connect_to_rethink(self):
      try:
        self.flock['r'] = rethinkdb
        # self.flock['r'].connect( "localhost", 28015).repl().reconnect()
        self.flock['r'].connect( host=self.db_config['rethinkdb_host'],port=self.db_config['rethinkdb_port'],user=self.db_config['rethinkdb_username'], password=self.db_config['rethinkdb_passwd']).repl().reconnect()

        print("connected to RethinkDB!")
      except Exception as e:
        print("Can not connect to RethinkDB : ")
        print("   %s" %(e))
        print("----------------------------------------------------")
###########################################################
###########################################################
# TABLE STRUCTURE in MARIADB
"""
-- --------------------------------------------------------

--
-- Table structure for table `cordis_projects`
--

CREATE TABLE `cordis_projects` (
  `rcn` int(10) UNSIGNED NOT NULL COMMENT 'Primary key of the Cordis entity',
  `id` int(10) UNSIGNED NOT NULL COMMENT 'Primary key of the Project entity',
  `acronym` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project Acronym.',
  `researcher` varchar(1024) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project Researcher.',
  `startdate` date DEFAULT NULL COMMENT 'Project Start Date.',
  `enddate` date DEFAULT NULL COMMENT 'Project End Date.',
  `reference` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project Reference.',
  `status` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project Status.',
  `title` varchar(2048) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project Title.',
  `teaser` varchar(4096) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project Teaser.',
  `objective` longtext CHARACTER SET utf8 COMMENT 'Project Objective.',
  `language` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project Language.',
  `availableLanguages` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project availableLanguages.',
  `ecMaxContribution` int(11) DEFAULT NULL COMMENT 'Project ecMaxContribution.',
  `totalCost` int(11) DEFAULT NULL COMMENT 'Project totalCost.',
  `contract_startDate` date DEFAULT NULL COMMENT 'Project Contract Start Date.',
  `contract_endDate` date DEFAULT NULL COMMENT 'Project Contract End Date.',
  `contract_duration` int(11) DEFAULT NULL COMMENT 'Project contract_duration.',
  `statusDetails` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project statusDetails.',
  `contentCreationDate` timestamp NULL DEFAULT NULL COMMENT 'Project contentCreationDate.',
  `contentUpdateDate` timestamp NULL DEFAULT NULL COMMENT 'Project contentUpdateDate.',
  `lastUpdateDate` timestamp NULL DEFAULT NULL COMMENT 'Project lastUpdateDate.',
  `relations` longtext CHARACTER SET utf8 COMMENT 'Project relations.',
  `hostInstitution_name` varchar(2048) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project hostInstitution_name.',
  `hostInstitution_country` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project hostInstitution_country.',
  `hostInstitution_url` varchar(2048) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project hostInstitution_url.',
  `esn_last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Project esn_last_updated.',
  `call_details` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project call_details.',
  `call_year` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project call_year.',
  `funding_scheme` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project funding_scheme.',
  `subpanel` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project subpanel.',
  `entity_type` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT 'Project entity_type.'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

"""


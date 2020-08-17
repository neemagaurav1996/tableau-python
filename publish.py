'''
Script to publish the tableau dashboard through API.
Use Python3.5 or above
'''
import os
import shutil 
from zipfile import ZipFile
import tableauserverclient as TSC
from tableauhyperapi import *

#Tableau server credentials
TABLEAU_USER='<TABLEAU_SERVER_USERNAME>'
TABLEAU_PASSWORD='<TABLEAU_SERVER_PSWORD>'
TABLEAU_SERVER='<TABLEAU_SERVER_URL>'
	
class Publish:

	def __init__(self,csv_path):
		#define the csv path, new workbook name and template workbook here
		self.csv_path = 'data.csv'
		self.wb_name = 'New'
		self.template_twbx = "template.twbx"

	#Unzip the zip file
	def unzipIt(self,file_name):
		with ZipFile(file_name, 'r') as zip: 
			zip.extractall(self.template_twbx)

	#Create a copy of existing template folder, Create new if already exists for particular ws
	def copyFolder(self):
		shutil.rmtree(self.wb_name, ignore_errors = True)
		shutil.copytree("template", self.wb_name) 
		shutil.rmtree("template", ignore_errors = True)

	#Generate hyper file from the new content
	def generateHyper(self):
		hyper_file_name = os.listdir(self.wb_name+'/Data/Extracts')[0]
		hyper_file_path = self.wb_name+'/Data/Extracts/'+hyper_file_name

		#Get definition (table schema) from the template hyper file
		with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU ) as hyper:

			with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
				template_definition = connection.catalog.get_table_definition(TableName('Extract', 'Extract'))

			table_definition = TableDefinition(
				table_name=TableName("Extract", "Extract"),
				columns = template_definition.columns
			)

			with Connection(endpoint=hyper.endpoint,database=hyper_file_path,create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
				connection.catalog.create_schema('Extract')
				connection.catalog.create_table(table_definition=table_definition)
				#Load all rows into table from the CSV file.
				num_rows = connection.execute_command(
					command=f"COPY {table_definition.table_name} from {escape_string_literal(CSV_FILE)} with "
					f"(format csv, NULL 'NULL', delimiter ',', header)")

	#The function will modify the twb file to change the workbook name inside the twb file
	#Other modifications can also be made here like changing title etc.
	def modifyTwb(self):

		twb_file_path = self.wb_name+'/'+TOOL_NAME+".twb"

		#Read and modify content
		with open(twb_file_path, "rt") as twb_file:
			data = twb_file.read()
			data = data.replace('template',self.wb_name)

		#Write new content
		with open(twb_file_path, "wt") as twb_file:
			twb_file.write(data)

	#Zip the stuff, change the extension to twbx and remove the folder
	def zipIt(self):
		shutil.make_archive(self.wb_name, 'zip', self.wb_name)
		os.rename(self.wb_name+".zip",self.wb_name+".twbx")
		shutil.rmtree(self.wb_name, ignore_errors = True)

	#Publish to Tableau server
	def publishToTableau(self):
		tableau_auth = TSC.TableauAuth(TABLEAU_USER, TABLEAU_PASSWORD)
		# tableau_auth = TSC.PersonalAccessTokenAuth('TOKEN_NAME','TOKEN_VALUE')
		server = TSC.Server(TABLEAU_SERVER)
		with server.auth.sign_in(tableau_auth):
			wb_item = TSC.WorkbookItem(name=self.wb_name,project_id="")
			wb_item = server.workbooks.publish(wb_item, self.wb_name+".twbx", 'Overwrite')
			print(wb_item.name)


if __name__ == '__main__':
	publish = Publish(CSV_FILE,WS_ID)
	publish.unzipIt(publish.template_twbx)
	publish.copyFolder()
	publish.generateHyper()
	publish.modifyTwb()
	publish.zipIt()
	publish.publishToTableau()

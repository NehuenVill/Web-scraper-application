import json,os,re,requests,sys,random,csv,time
from bs4 import BeautifulSoup

class Helper():

	def __init__(self):
		pass

	def reading_csv(self,csv_filename):
		f = open(csv_filename,'r',encoding='utf-8',errors='replace')
		csv_data = []
		reader = csv.reader(f)
		for row in reader:
			csv_data.append(row)

		f.close()
		return csv_data 

	def writing_csv(self,data,csv_filename):

		myFile = open(csv_filename, 'w', newline='',encoding='utf-8',errors='replace')
		with myFile:
			writer = csv.writer(myFile)
			writer.writerows(data)

		return csv_filename

	def writing_output_file(self,sub_list,headers,file_name):

		if self.is_file_exist(file_name):
			csv_data = self.reading_csv(file_name)
		else:
			csv_data = []
			csv_data.append(headers)

		csv_data.extend(sub_list)

		self.writing_csv(csv_data,file_name)
		print('-------------Writing Output File Done-----------------')

	def writing_output_file_json(self,sub_dict,file_name):

		if self.is_file_exist(file_name):
			json_data = self.read_json_file(file_name)
		else:
			json_data = []

		json_data.append(sub_dict)

		self.write_json_file(json_data,file_name)
		print('-------------Writing JSON Output File Done-----------------')

	def get_timestamp(self):
		return '23_05_2022'
		# return time.strftime('%d_%m_%Y')

	def checking_folder_existence(self,dest_dir):
		if not os.path.exists(dest_dir):
			os.mkdir(dest_dir)
			print("Directory " , dest_dir ,  " Created ")
		else:
			pass
			#print("Directory " , dest_dir ,  " Exists ")

		return dest_dir
		
	def write_json_file(self,data,filename):

		with open(filename, 'w') as outfile:
			json.dump(data, outfile,indent=4)

	def read_json_file(self,filename):
		data = {}
		with open(filename) as json_data:
			data = json.load(json_data)
		return data

	def is_file_exist(self,filename):
		if os.path.exists(filename):
			return True
		else:
			return False

	def list_all_files(self,directory,extension):
		all_files = []
		for file in os.listdir(directory):
			if file.endswith(extension):
				all_files.append(os.path.join(directory, file))
		return all_files

	def write_random_file(self,text,file_name):
		file= open(file_name,"w")
		file.write(str(text))
		file.close()

	def json_exist_data(self,fileName):
		json_data = []
		if self.is_file_exist(fileName):
			json_data = self.read_json_file(fileName)
		return json_data

	def read_random_file(self,file_name):
		f = open(file_name, "r")
		return f.read()
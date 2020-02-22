from datetime import datetime
from os import listdir
from application_logging.logger import App_Logger
import pandas as pd


class dataTransform:

     """
               This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

               Written By: iNeuron Intelligence
               Version: 1.0
               Revisions: None

               """

     def __init__(self):
          self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
          self.logger = App_Logger()


     def addQuotesToStringValuesInColumn(self):
          """
                                           Method Name: addQuotesToStringValuesInColumn
                                           Description: This method converts all the columns with string datatype such that
                                                       each value for that column is enclosed in quotes. This is done
                                                       to avoid the error while inserting string values in table as varchar.

                                            Written By: iNeuron Intelligence
                                           Version: 1.0
                                           Revisions: None

                                                   """

          log_file = open("Training_Logs/addQuotesToStringValuesInColumn.txt", 'a+')
          try:
               onlyfiles = [f for f in listdir(self.goodDataPath)]
               for file in onlyfiles:
                    data = pd.read_csv(self.goodDataPath+"/" + file)
                    #data = self.removeHyphenFromColumnNames(data)
                    # for col in data.columns:
                    #      # if col in column: # add quotes in string value
                    #      data[col] = data[col].apply(lambda x: "'" + str(x) + "'")
                         # if col not in column: # add quotes to '?' values in integer/float columns
                    for column in data.columns:
                         count = data[column][data[column] == '?'].count()
                         if count != 0:
                              data[column] = data[column].replace('?', "'?'")
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    #csv['Wafer'] = csv['Wafer'].str[6:]
                    data.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                    self.logger.log(log_file," %s: Quotes added successfully!!" % file)
               #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")
          except Exception as e:
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
               #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
               log_file.close()
          log_file.close()

    # def removeHyphenFromColumnNames(self,data):
     #      """
     #                                                Method Name: addQuotesToStringValuesInColumn
     #                                                Description: This method changing the column names by replacing the '-'.
     #
     #                                                 Written By: iNeuron Intelligence
     #                                                Version: 1.0
     #                                                Revisions: None
     #
     #                                                        """
     #      log_file = open("Training_Logs/removeHyphenFromColumnNames.txt", 'a+')
     #      try:
     #
     #           # there are "hyphen" in our column name which results in failure when inserting the column names in the table
     #           # so we are changing the column names by replacing the '-'
     #           for col in data.columns:
     #                new_col = col.replace('-', '')
     #                data.rename(columns={col: new_col},inplace=True)
     #           return data
     #
     #      except Exception as e:
     #           self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
     #           #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
     #           log_file.close()
     #      log_file.close()
     #      return data
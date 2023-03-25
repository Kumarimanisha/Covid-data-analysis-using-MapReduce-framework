########################IMPORTING REQUIRED LIBRARIES ###############
import pandas as pd 
import sys
from sklearn.impute import KNNImputer
#######################################################

'''function to read csv fileas as pandas  dataframe'''
def read_input_data():
    try:
        print("==============Reading index file ============='")
        df_index_file=pd.read_csv("https://storage.googleapis.com/covid19-open-data/v3/index.csv") # reading index file 
        print(df_index_file.head())#displaying sample records for index file
        print("==============Reading age file ============='") 
        df_age_file=pd.read_csv("https://storage.googleapis.com/covid19-open-data/v3/by-age.csv",dtype={148:'str',149:'str',150:'str',151:'str'}) # reading age stratified dataset
        print(df_age_file.head())#displaying sample records for index file
        print("==============Reading gender file ============='")
        df_sex_file=pd.read_csv("https://storage.googleapis.com/covid19-open-data/v3/by-sex.csv")# reading gender stratified dataset
        print(df_sex_file.head())#displaying sample records for index file
        return (index_file_eda(df_index_file,df_age_file,df_sex_file))
        
    except Exception as e:
        print ("Error occurred in while reading the input files")
        sys.exit(1)

    
'''function to clean and select the required columns for the index file'''

def index_file_eda(df_index_file,df_age_file,df_sex_file):    
    try:
        list(df_index_file) # checking list of columns in the index dataframe
        df_index_file.shape #checking rows and columns of the index dataframe
        df_index_file.drop(df_index_file.columns[[1,2,3,6,7,8,9,10,11,12,13]],axis=1,inplace=True) #dropping irrelevant columns from the index dataframe
    
        df_loc_key_cntry=df_index_file[df_index_file["aggregation_level"]==1] # creating a new dataframe with aggregation_level set as 1 as data at province level will be analyzed in the project 
        df_loc_key_cntry=df_loc_key_cntry[(df_loc_key_cntry["country_name"]=='United States of America') | (df_loc_key_cntry["country_name"]=='Spain') | (df_loc_key_cntry["country_name"]=='Germany')] # selecting countries to be analyzed on
        df_age_file.shape
        df_age_file=df_age_file[['date','location_key','new_confirmed_age_0','new_confirmed_age_1','new_confirmed_age_2','new_confirmed_age_3','new_confirmed_age_4','new_confirmed_age_5','new_confirmed_age_6','new_confirmed_age_7','new_confirmed_age_8','new_confirmed_age_9','new_deceased_age_0','new_deceased_age_1','new_deceased_age_2','new_deceased_age_3','new_deceased_age_4','new_deceased_age_5','new_deceased_age_6','new_deceased_age_7','new_deceased_age_8','new_deceased_age_9']]  # keeping only relevant columns in the age dataframe 
        df_age_file['date']=pd.to_datetime(df_age_file['date'],dayfirst=True,errors='coerce')    # correcting the incorrect format of date column in the daatset
        df_age_file.dropna(subset=['date'],inplace=True) 
        df_age_file['date']=df_age_file['date'].dt.strftime('%Y-%m')#updating the date format to yyyy-mm to make the dataset consistent
        df_age_file=df_age_file[df_age_file['date']>='2020-01'] # dropping all the records with date earlier than 2020-01
        print(df_age_file.isnull().sum()*100/len(df_age_file)) # checking % of null values in teh age dataframe 
    
    
        df_sex_file=df_sex_file[['date', 'location_key', 'new_confirmed_male', 'new_confirmed_female','new_deceased_male', 'new_deceased_female']] # keeping only relevant columns in the gender dataframe 
        df_sex_file['date']=pd.to_datetime(df_sex_file['date'],dayfirst=True,errors='coerce')      # correcting the incorrect format of date column in the daatset
        df_age_file.dropna(subset=['date'],inplace=True) 
        df_sex_file['date']=df_sex_file['date'].dt.strftime('%Y-%m')#updating the date format to yyyy-mm to make the dataset consistent
        df_sex_file=df_sex_file[df_sex_file['date']>='2020-01'] # dropping all the records with date earlier than 2020-01
        print(df_sex_file.isnull().sum()*100/len(df_sex_file)) # checking % of null values in teh age dataframe 
        return dataframe_merge(df_loc_key_cntry,df_age_file,df_sex_file)
        
    except Exception as e:
        print ("Error occurred in while cleaning the dataframes")
        sys.exit(1)

    '''function to merge the age and gender datsets with index file and perform necessary transformations to get the final dataframe to run mapreduce function on'''
    
def dataframe_merge(df_loc_key_cntry,df_age_file,df_sex_file):
    try:
        df_age_file_cntry_fltrd=pd.merge(df_loc_key_cntry,df_age_file,how='inner',on='location_key') # joining index file with age dataset on column 'location_key'
        df_age_file_cntry_fltrd=df_age_file_cntry_fltrd[['location_key', 'country_code','country_name','date','new_confirmed_age_0', 'new_confirmed_age_1', 'new_confirmed_age_2', 'new_confirmed_age_3', 'new_confirmed_age_4', 'new_confirmed_age_5', 'new_confirmed_age_6', 'new_confirmed_age_7', 'new_confirmed_age_8', 'new_confirmed_age_9', 'new_deceased_age_0','new_deceased_age_1', 'new_deceased_age_2', 'new_deceased_age_3', 'new_deceased_age_4', 'new_deceased_age_5', 'new_deceased_age_6', 'new_deceased_age_7', 'new_deceased_age_8', 'new_deceased_age_9']]
        #imputing  the NULL values in age dataframe using knn imputer
        imputer = KNNImputer(n_neighbors=1,weights='uniform',metric='nan_euclidean') # creating knn imputer object with nearest neighbot set as 1
        df_age_file_cntry_fltrd['new_deceased_age_0']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_deceased_age_0']])
        df_age_file_cntry_fltrd['new_deceased_age_1']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_deceased_age_1']])
        df_age_file_cntry_fltrd['new_deceased_age_2']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_deceased_age_2']])
        df_age_file_cntry_fltrd['new_deceased_age_3']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_deceased_age_3']])
        df_age_file_cntry_fltrd['new_deceased_age_4']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_deceased_age_4']])
        df_age_file_cntry_fltrd['new_deceased_age_5']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_deceased_age_5']])
        df_age_file_cntry_fltrd['new_deceased_age_7']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_deceased_age_7']])
        df_age_file_cntry_fltrd['new_deceased_age_8']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_deceased_age_8']])
        df_age_file_cntry_fltrd['new_deceased_age_9']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_deceased_age_9']]) 
        
        df_age_file_cntry_fltrd['new_confirmed_age_0']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_0']])
        df_age_file_cntry_fltrd['new_confirmed_age_1']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_1']])
        df_age_file_cntry_fltrd['new_confirmed_age_2']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_2']])
        df_age_file_cntry_fltrd['new_confirmed_age_3']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_3']])
        df_age_file_cntry_fltrd['new_confirmed_age_4']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_4']])
        df_age_file_cntry_fltrd['new_confirmed_age_5']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_5']])
        df_age_file_cntry_fltrd['new_confirmed_age_6']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_6']])
        df_age_file_cntry_fltrd['new_confirmed_age_7']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_7']])
        df_age_file_cntry_fltrd['new_confirmed_age_8']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_8']])
        df_age_file_cntry_fltrd['new_confirmed_age_9']=imputer.fit_transform(df_age_file_cntry_fltrd[['new_confirmed_age_9']])
        
        #creating age bins 
        df_age_file_cntry_fltrd['confirmed_young_children'] = df_age_file_cntry_fltrd['new_confirmed_age_0']
        df_age_file_cntry_fltrd['confirmed_youth']= df_age_file_cntry_fltrd['new_confirmed_age_1']+df_age_file_cntry_fltrd['new_confirmed_age_2']
        df_age_file_cntry_fltrd['confirmed_young_adults']= df_age_file_cntry_fltrd['new_confirmed_age_3']+df_age_file_cntry_fltrd['new_confirmed_age_4']
        df_age_file_cntry_fltrd['confirmed_middle_aged_adults']= df_age_file_cntry_fltrd['new_confirmed_age_5']+df_age_file_cntry_fltrd['new_confirmed_age_6']
        df_age_file_cntry_fltrd['confirmed_seniors']= df_age_file_cntry_fltrd['new_confirmed_age_7']+df_age_file_cntry_fltrd['new_confirmed_age_8']+df_age_file_cntry_fltrd['new_confirmed_age_9']
        df_age_file_cntry_fltrd['deceased_young_children'] = df_age_file_cntry_fltrd['new_deceased_age_0']
        df_age_file_cntry_fltrd['deceased_youth']= df_age_file_cntry_fltrd['new_deceased_age_1']+df_age_file_cntry_fltrd['new_deceased_age_2']
        df_age_file_cntry_fltrd['deceased_young_adults']= df_age_file_cntry_fltrd['new_deceased_age_3']+df_age_file_cntry_fltrd['new_deceased_age_4']
        df_age_file_cntry_fltrd['deceased_middle_aged_adults']= df_age_file_cntry_fltrd['new_deceased_age_5']+ df_age_file_cntry_fltrd['new_deceased_age_6']
        df_age_file_cntry_fltrd['deceased_seniors']=df_age_file_cntry_fltrd['new_deceased_age_7']+df_age_file_cntry_fltrd['new_deceased_age_8']+df_age_file_cntry_fltrd['new_deceased_age_9']
        #dropping old unnecessary columns
        df_age_file_cntry_fltrd.drop(df_age_file_cntry_fltrd.columns[[4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]],axis=1,inplace=True)
        #melting the dataframe to create new variable category with numebr of cases as values
        df_age_file_cntry_fltrd=pd.melt(df_age_file_cntry_fltrd,id_vars=["location_key", "country_code","country_name","date"],var_name="category",value_name="Number_of_patients")
        ########################################   
    
        df_sex_file_cntry_fltrd=pd.merge(df_loc_key_cntry,df_sex_file,how='inner',on='location_key') # joining index file with gender dataset on column 'location_key'
        #imputing  the NULL values in age dataframe using knn imputer
        df_sex_file_cntry_fltrd['new_confirmed_male']=imputer.fit_transform(df_sex_file_cntry_fltrd[['new_confirmed_male']])
        df_sex_file_cntry_fltrd['new_confirmed_female']=imputer.fit_transform(df_sex_file_cntry_fltrd[['new_confirmed_female']])
        df_sex_file_cntry_fltrd['new_deceased_male']=imputer.fit_transform(df_sex_file_cntry_fltrd[['new_deceased_male']])
        df_sex_file_cntry_fltrd['new_deceased_female']=imputer.fit_transform(df_sex_file_cntry_fltrd[['new_deceased_female']])
        #creating new gender bins
        df_sex_file_cntry_fltrd['confirmed_male_case']=df_sex_file_cntry_fltrd['new_confirmed_male']
        df_sex_file_cntry_fltrd['confirmed_female_case']=df_sex_file_cntry_fltrd['new_confirmed_female']
        df_sex_file_cntry_fltrd['deceased_male_case']=df_sex_file_cntry_fltrd['new_deceased_male']
        df_sex_file_cntry_fltrd['deceased_female_case']=df_sex_file_cntry_fltrd['new_deceased_female']
        #dropping old unnecessary columns
        df_sex_file_cntry_fltrd=df_sex_file_cntry_fltrd[['location_key','country_code','country_name','date','confirmed_male_case', 'confirmed_female_case', 'deceased_male_case', 'deceased_female_case']]
        #melting the dataframe to create new variable category with numebr of cases as values
        df_sex_file_cntry_fltrd=pd.melt(df_sex_file_cntry_fltrd,id_vars=["location_key", "country_code","country_name","date"],var_name="category",value_name="Number_of_patients")    
        return df_age_file_cntry_fltrd,df_sex_file_cntry_fltrd
        
    except Exception as e:
        print ("Error occurred in while reading the transforming the cleaned dataframes")
        sys.exit(1)
  
if __name__ == '__main__':

    print("==================Reading the input files======================= ")
    cleaned_age_dataframe,cleaned_gender_dataframe=read_input_data()
    print("===================Saving the reulatnt cleanded dataframe as text file=======================")
    cleaned_gender_dataframe.to_csv('input_gender_file.txt', sep ='\t',index=False,header=False)
    cleaned_age_dataframe.to_csv('input_age_file.txt', sep ='\t',index=False,header=False)

    
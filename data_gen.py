from pyspark.sql import SparkSession
import pandas
from collections import defaultdict
import random
from math import ceil

'''
Process used to create Data

According to the use case mentioned we need to generate data that had 
1. skew distribution : size s1
2. normal distribution : size s1
3. skew distribution : size s2
4. normal distribution : size s2

Here s1 and s2 are the sizes that can vary depend on use case for our e.g we had taken s1 = 10GB and s2 = 25GB

--> To tackle this I had created class called writegenerateData which can write and generate the data with the size that we want 
    for this there is function called generate_huge_data which generates the data with the specified size

For the scenario part we had four scenarios with us 
1. Data Skew
2. Input Data Increased
3. Input Data Increased, Skewed 
4. Less resources

class create_scenarios helps us to create the wanted scenarios where we first load the desired data and then apply the load_and_groupBy function

'''

class eshtablishContext:
    def __init__(self,session_name):
        self.session = None
        self.session_name = session_name

    def build_session(self):
        self.session = SparkSession.builder.appName(self.session_name).enableHiveSupport().getOrCreate()
        return self.session
    
    def return_data(self,dbname,table_name):
        self.session.sql("use "+dbname)
        return self.session.sql("select * from "+table_name)
    
    def load_skew_data(self,table_name):
        return self.return_data(table_name)

    def load_1_gb(self,table_name):
        return self.return_data(table_name)
    
    def load_2_gb(self,table_name):
        return self.return_data(table_name)
    
    def do_groupby(self,df,col1,col2):
        return df.groupBy(col1).sum(col2)

class create_scenarios:
    def create_out_of_mem_driver(self):
        '''
        This will create OOM problem tested with 5 executors and 512mb mem
        '''
        use = eshtablishContext("gen_oom")
        session = use.build_session()
        df = session.sql("use etl_pipeline")
        df = session.sql("select * from 1gb_data")
        lst = df.collect()
        return len(lst)
    
    def create_out_of_mem_executor(self):
        '''
        This will create OOM problem tested with 1 executors and 512mb mem
        '''
        session = use.build_session()
        df = session.sql("use data_gen")
        df = session.sql("select * from skew_dist_1")
        for i in range(100):
            df = df.union(df)
            print("Done union with df for iteration :: ",i)
        session.sql("use bigdata")
        df.write.mode('append').saveAsTable("bigdata.data")    
        print("Converted Pyspark DataFrame to Pandas DataFrame.")
    
    def test_execution_time_diff(self,use,session):
        import time
        df = session.sql("use etl_pipeline")
        start = time.time()
        start = time.time()
        df2 = session.sql("select * from 2gb_data")
        use.do_groupby(df2,"marital","duration").show(truncate=False)
        print("Time taken to read 2GB data :: ",time.time()-start)
    
    def load_and_groupBy(self,session,dbname,tabname,col1,col2):
        import time
        session.sql('use ' + dbname)
        df = session.sql('select * from ' + tabname)
        start = time.time()
        df.groupBy(col1).sum(col2).show(truncate=False)
        print("Time taken for {} is {} ".format(tabname,time.time()-start))
    
    def check_dist(self,session,dbname,tabname,col1):
        import time
        session.sql('use ' + dbname)
        df = session.sql('select * from ' + tabname)
        start = time.time()
        df.groupBy(col1).count().show()
        print("Time taken for {} is {} ".format(tabname,time.time()-start))
    
    def load(self,session,dbname,tabname):
        import time
        session.sql('use ' + dbname)
        start = time.time()
        df = session.sql('select * from ' + tabname)
        print("Time taken for {} is {} ".format(tabname,time.time()-start))


'''
can use this class generate_huge_data with the dataframe that you would like to replicate
'''

class writegenerateData:
    def __init__(self,lim):
        self.limit = lim
        self.lst = []
        self.cnt = 0
        self.df = None
    
    def fetch_data(self,url):
        self.df = pandas.read_csv(url,sep=';')

    def get_df_memsize(self,df):
        sample = df.limit(1).toPandas().memory_usage(index=True).sum()
        val = df.count() * sample
        return val
    
    def generate_small_user_dist_df(self,col_name,values,offset):
        ndf = pandas.DataFrame(columns = self.df.columns.tolist())
        for i in range(len(values)):
            value = values[i]
            for i in self.df.loc[self.df[col_name]==value].values.tolist()[:offset[i]]:
                ndf.loc[len(ndf)] = i
        return ndf
    
    def compute_avg_size(self,df,theta):
        avg_size = 0
        denom = int(theta*100)
        iterations = (100+denom)//denom
        print(iterations,denom)
        for _ in range(iterations):
            avg_size = avg_size + df.sample(theta).limit(1).toPandas().memory_usage(index=True).sum()
        avg_size = avg_size//iterations
        return avg_size * df.count()


    
    def random_sample_generator(self,session):
        df = session.createDataFrame(self.df)
        lst = df.sample(fraction = 0.06).collect()
        hsh = defaultdict(list)
        features = df.columns
        custom_df = pandas.DataFrame(columns=features)
        for row in lst:
            for key in features:
                hsh[key].append(row[key])
        for _ in range(df.count()):
            lst = []
            for key in hsh:
                lst.append(random.choice(hsh[key]))
            custom_df.loc[len(custom_df.index)] = lst
        print(custom_df.head())
        spdf = session.createDataFrame(custom_df)
        print("============================ Generated spark df with random sampling ====================")
        print(self.compute_avg_size(spdf,0.06))
    
    def return_dflist(self):
        return self.lst
    
    def commit_data_to_db(self,df,db_string):
        df.write.mode("append").saveAsTable(db_string)
        
    def write_to_db(self,db_string,parallel_exec=False):
        if parallel_exec:
            self.write_parallel_to_db(db_string)
        else:
            for df in self.lst:
                self.commit_data_to_db(df,db_string)
            
    def write_parallel_to_db(self,db_string):
        import threading
        threads = [threading.Thread(target=self.commit_data_to_db, args=[(db,db_string) for db in self.lst] )]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    
    def generate_huge_data(self,session,dbname,tab_name,size,theta):
        from math import ceil
        df = session.sql("use " + dbname)
        df = session.sql("select * from " + tab_name)
        sample = df.limit(theta)
        chunk_size = sample.toPandas().memory_usage(index=True).sum()
        sp = self.get_df_memsize(df)
        print("=================== size are =============== :: ",sp,size)
        sz = (size - sp)//chunk_size
        print("============== Wanted to create partitions of {} of size {} ".format(sz,chunk_size))
        for i in range(sz):
            sample.write.mode("append").saveAsTable(dbname+'.'+tab_name)
            print("============= Done with data insertion at iteration {}==============".format(i))
        
    def calc_df_size(self,session,dbname,tab_name):
        df = session.sql("use " + dbname)
        df = session.sql("select * from " + tab_name)
        return self.get_df_memsize(df)

    def store_data(self,session,df,dbname,tab_name):
        spdf = session.createDataFrame(df)
        spdf.write.mode("append").saveAsTable(dbname+'.'+tab_name)
    
    def see_partitions(self,session,dbname,tab_name):
        import pyspark.sql.functions as F
        session.sql("use " + dbname)
        df = session.sql("select * from " + tab_name)
        print("================ Showing partitions ================")
        print(df.groupBy(F.spark_partition_id()).count().show())
    
    def see_distribution(self,session,dbname,tab_name,colname):
        session.sql("use " + dbname)
        df = session.sql("select * from " + tab_name)
        print("================ Showing data in DB ================")
        print(df.groupBy(colname).count().show())



use = eshtablishContext("Increase_data")
session = use.build_session()
datalib = writegenerateData(int(1e6))
scene = create_scenarios()



session.sql("use skew_data")
df = session.sql("select * from skew_data")

for _ in range(2):
    df.write.mode("append").saveAsTable("skew_data"+'.'+"skew_data")


#datalib.see_partitions(session,"skew_data","skew_data_1")
#datalib.see_distribution(session,"skew_data","skew_data_1","marital")

#datalib.generate_huge_data(session,'bigdata','equal_data',10*int(1e9),int(1e6))





# Can call the necessary functions according to the need
#scene.load_and_groupBy(session,'bigdata','equal_data','marital','duration')

#scene.create_out_of_mem_executor()




'''
# Misc Section 

gen_data = writegenerateData(int(1e6))
gen_data.increase_store_data(session,"etl_pipeline","2gb_data",2,int(1e5))
df = use.return_data("etl_pipeline","2gb_data")
print(datalib.get_df_memsize(df))C


#df = session.sql('select * from skew_dist')
#df = df.sample(fraction = 0.79)
#df.write.mode("overwrite").saveAsTable('data_gen'+'.'+'skew_dist_1')

#datalib.fetch_data("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv")
#normal_df = datalib.generate_small_user_dist_df('marital',['married','single'],[49,51])
#skew_df = datalib.generate_small_user_dist_df('marital',['married','single'],[10,90])
#print("=======================Going to generate data with required distribution=========================")
#datalib.store_data(session,skew_df,'data_gen','skew_dist')
#print("====================== Stored the data ==============")
#datalib.generate_huge_data(session,'data_gen','skew_dist',10*int(1e9),1000,1000)
#print("=====================Generated the Dataset :: ",datalib.calc_df_size(session,'data_gen','skew_dist_1'))

def increase_and_store_data(self,session,dbname,tab_name,offset,theta):
        df = session.sql("use " + dbname)
        df = session.sql("select * from " + tab_name)
        sample = df.limit(theta)
        for _ in range(offset):
            df = df.union(sample)
        df = df.union(df)
        df.write.mode("append").saveAsTable(dbname+'.'+tab_name)


#datalib.fetch_data("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv")
#scene = create_scenarios()
#scene.load_and_groupBy(session,'data_gen','normal_dist','marital','duration')
#scene.load_and_groupBy(session,'data_gen','skew_dist_1','marital','duration')
#scene.check_dist(session,'data_gen','normal_dist','marital')
#print("=========================== Finished =========================")

#scene.load_and_groupBy(session,'data_gen','skew_dist_1','marital','duration')



#normal_df = datalib.generate_small_user_dist_df('marital',['married','single'],[49,51])
#print("======================= Going to generate data with required distribution =========================")
#datalib.store_data(session,normal_df,'data_gen','normal_dist_1')
#scene.load_and_groupBy(session,'data_gen','normal_dist_1','marital','duration')
#datalib.generate_huge_data(session,'data_gen','skew_dist',25*int(1e9),int(1e6))
#print("=====================Generated the Dataset :: ",datalib.calc_df_size(session,'data_gen','skew_dist'))

df1 = session.sql("select * from 1gb_data")
        use.do_groupby(df1,"marital","duration").show(truncate=False)
        print("Time taken to read 1GB data :: ",time.time()-start)

'''

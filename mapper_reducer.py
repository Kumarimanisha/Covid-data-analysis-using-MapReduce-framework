from mrjob.job import MRJob
from mrjob.step import MRStep

class CovidCasesBreakdown(MRJob):
    def steps(self):
        return [MRStep (mapper=self.mapper_get_patients,reducer=self.reducer_total_patients) ]

    def mapper_get_patients(self, _, line):
        
        (location_key,country_code,country_name,date,category,Number_of_patients)= line.split('\t')
        yield location_key+'~'+country_name+'~'+category+'~'+date,float(Number_of_patients)

    def reducer_total_patients(self,key,values):
        yield key, sum(values)

if __name__ == "__main__":
    CovidCasesBreakdown.run()
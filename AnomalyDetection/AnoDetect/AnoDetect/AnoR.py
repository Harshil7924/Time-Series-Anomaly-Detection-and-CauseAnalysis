import rpy2.robjects as ro

def R(x):
	return ro.r(x)
R("library(AnomalyDetection)")
R("data(raw_data)")

R("res = AnomalyDetectionTs(raw_data, max_anoms=0.02, direction='both')")
df = R("res[1]")[0]
R("write.csv(res[['anoms']], file='out',row.names=FALSE)")
R("write.csv(raw_data, file='outraw.csv',row.names=FALSE,sep=',')")

articles=R("read.csv(file='out',head=TRUE,sep=',',stringsAsFactors=FALSE)")
print articles
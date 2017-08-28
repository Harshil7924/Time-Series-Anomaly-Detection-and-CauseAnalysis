# args[1] - filename to read
# args[2] - Hour | Minute | Day
# args[3] - model name 'Additive' | 'Multiplicative'
# args[4] - max_anoms
# args[5] - alpha
# args[6] - comma separated column names
# args[7] - drill down options
# args[8] - tsformat

library(sqldf)
library(zoo)
library(imputeTS)
library(plyr)
freq <- 24
model <- 'a'
maxanoms <- 0.49
alpha <- 0.05


add <- function(A, B) {
	if(model == 'Additive') {
		return (A+B)
	}
	return (A*B);
}

remove <- function(A,B) {
	if(model == 'Additive') {
		return (A-B)
	}
	return (A/B);
}
detect <- function(df, maxanoms, alpha) {
	#Formatting data
	df <- aggregate(value~time, sum, data=df)
	df1.zoo<-zoo(df[,-1],df[,1])
	df2 <- merge(df1.zoo,zoo(,seq(start(df1.zoo),end(df1.zoo),by=args[2])), all=TRUE)
	df <- data.frame(time = index(df2), value = coredata(df2))

	#fill NAs
	df[[2]] <- na.fill(df[[2]],0)
	
	trend = runmed(df[[2]], freq)

	# trend <- numeric(length=nrow(df))
	# ptr <-1
	# piecewiseperiod = 7*freq
	# while(ptr <= nrow(df)) {
	# 	end <- min(ptr+piecewiseperiod-1,nrow(df))
	# 	trend[ptr:end] <- median(df[ptr:end,2])
	# 	ptr <- ptr + piecewiseperiod
	# }

	detrend <- remove(df[[2]] , as.vector(trend))
	lim <- floor(length(detrend)/freq) * freq
	detrend <- detrend[1:lim]

	m <- t(matrix(data = detrend, nrow = freq))
	seasonal <- colMeans(m, na.rm = T)

	temp<-seasonal
	seasonal <- 1:length(trend)
	cur <- 1
	for(i in 1:length(seasonal)) {
		seasonal[i] = temp[cur]
		cur <- cur +1
		if(cur > length(temp)) 
			cur <- 1
	}

	obs <- remove(df[[2]], add(median(df[[2]]),seasonal))

	n <- length(obs)
	r <- trunc(maxanoms*n)
	outlier_ind <- numeric(length=r) 
	m <- 0 
	obs_new <- obs 
	
	for(i in 1:r){
		
		MAD <- mad(obs_new)
		if(is.na(MAD))
			break
		if(MAD == 0)
			break
		z <- abs(obs_new - median(obs_new))/MAD 
		
		max_ind <- which(z==max(z),arr.ind=T)[1] 
		R <- z[max_ind]
		outlier_ind[i] <- which(obs_new[max_ind] == obs, arr.ind=T)[1] 
		obs_new <- obs_new[-max_ind]
		p <- 1 - alpha/(2*(n-i+1)) 
		t_pv <- qt(p,df=(n-i-1)) 
		lambda <- ((n-i)*t_pv) / (sqrt((n-i-1+t_pv^2)*(n-i+1)))
		
		if (R > lambda) {
			m <- i
		}
	}
	outlier_ind <- outlier_ind[1:m]
	ret = df[outlier_ind,]
	ret = data.frame(timestamp = as.POSIXlt(ret[[1]]),value = ret[[2]])
	ret = ret[order(ret[[1]]),]
	return (ret)	
}

args <- commandArgs(TRUE)

filename <- args[1]
if(args[2] == "Hour"){
	freq <- 24*7
	args[2] <- "hour"
}
if(args[2]=="Minute") {
	freq <- 24*60
	args[2] <- "min"
}
if(args[2]=="Day")  {
	freq <- 7
	args[2] <- "day"
}
model <- args[3]
maxanoms <- as.numeric(as.character(args[4]))
alpha <- as.numeric(as.character(args[5]))

print(args[7])
f <- file(paste("files/",filename,sep=""))
sqlQuery <- paste('Select ',args[6],' from f ',sep="")
if(!is.na(args[7]) && args[7]!="") {
	sqlQuery <- paste(sqlQuery,'where ',as.character(args[7]),sep="")
}
print(sqlQuery)
data <- sqldf(sqlQuery, stringsAsFactors = FALSE,dbname = tempfile(), file.format =list(header = T, row.names = F))
if(nrow(data)>0){
	data[[1]] <- as.POSIXct(strptime(data[[1]], args[8]))
	data <- data[order(data[[1]]),]
	for(i in 2:ncol(data)) {
		data[[i]] <- as.numeric(as.character(data[[i]]))
	}
}
feature <- colnames(data)
colnames(data)[1] <- "time"
data <- ddply(data,.(time),colwise(sum))
colnames(data)[1] <- feature[1]
F = ncol(data)
print(head(data))

for(i in 2:F){
	if(nrow(data) >0){
		data[[i]] <- as.numeric(as.character(data[[i]]))
		anoms = detect(data.frame(time = data[[1]],value = data[[i]]), maxanoms, alpha)
		S = paste("anomalies/","result_",feature[i],"drilled.csv", sep="")
		anoms = na.omit(anoms)
		write.csv(anoms, file=S,row.names=FALSE)
	}
	else{
		anoms = data.frame(timestamp = c(NA),value = c(NA))
		anoms = na.omit(anoms)
		S = paste("anomalies/","result_",feature[i],"drilled.csv", sep="")
		write.csv(anoms, file=S,row.names=FALSE)
	}
}
write.csv(data, file = paste("files/drilled.csv"),row.names=FALSE)
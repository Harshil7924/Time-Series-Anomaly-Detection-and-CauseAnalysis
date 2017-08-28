

# args[1] - filename to read
# args[2] - Hour | Minute | Day
# args[3] - model name 'Additive' | 'Multiplicative'
# args[4] - max_anoms
# args[5] - alpha
# args[6] - target value
# args[7] - drill down options
# args[8] - categorical features
# args[9] - numeric feature 
# args[10] - timestamp
# args[11] - tsformat

library(sqldf)
library(zoo)
library(imputeTS)
library(plyr)
library(hashmap)
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
detect <- function(df, maxanoms, alpha, getScore, metric) {
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
	# piecewiseperiod = freq
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

	# trend <- numeric(length=nrow(df))
	# ptr <-1
	# piecewiseperiod <- freq+1
	# if(args[2]=="min"){
	# 	piecewiseperiod <- 7*freq+1
	# }
	# while(ptr <= nrow(df)) {
	# 	end <- min(ptr+piecewiseperiod-1,nrow(df))
	# 	trend[ptr:end] <- median(df[ptr:end,2])
	# 	ptr <- ptr + piecewiseperiod
	# }
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
	outlier_ind <- unique(outlier_ind)

	ret = df[outlier_ind,]

	
	if(getScore) {
		ret = data.frame(timestamp = as.POSIXlt(ret[[1]]),value = ret[[2]])
		j <- 1
		for(i in outlier_ind) {
			if(length(trend[i])!=0 && length(seasonal[i])!=0 ) {
				score <- (df[i,2] - add(trend[i], seasonal[i]))/add(trend[i] , seasonal[i])
				score <- (score*100)
				if(is.na(score)) {
					ret[j,2] <- ""
				}
				else{
					if(score<0) {
						ch <- "MINUS"
					}
					else{
						ch <- "PLUS"
					}
					score<-abs(score)
					score <- sprintf("%.2f",score)
					temp <- paste("BOLDO",score,"%BOLDC",ch,"NEWLINE Actual ",metric," --> ",sprintf("%.2f",df[i,2]),"NEWLINE Expected ",metric," --> ",sprintf("%.2f",add(trend[i],seasonal[i])),sep="")
					ret[j,2] <- temp
				}
			}
			else{
				score <- NA
				ret[j,2] <- ""
			}
			j <- (j+1)
		}
	}
	else{
		ret = data.frame(timestamp = as.POSIXlt(ret[[1]]),value = ret[[2]])
	}
	ret = ret[order(ret[[1]]),]
	return (ret)	
}

args <- commandArgs(TRUE)

filename <- args[1]
if(args[2] == "Hour"){
	freq <- 24
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
target <- args[6]
timestamp <- args[10]
tsformat <- args[11]
f <- file(paste("files/",filename,sep=""))
sqlQuery <- 'Select *  from f'
if(!is.na(args[7]) && args[7]!="") {
	sqlQuery <- paste(sqlQuery,'where ',as.character(args[7]),sep="")
}


data <- sqldf(sqlQuery, stringsAsFactors = FALSE,dbname = tempfile(), file.format =list(header = T, row.names = F))
if(nrow(data)>0){
	data[[timestamp]] <- as.POSIXct(strptime(data[[timestamp]], tsformat))
	data <- data[order(data[[timestamp]]),]
	# names <- colnames(data)
	# for(i in 1:ncol(data)) {
	# 	if(names[i] != timestamp)
	# 		data[[i]] <- as.numeric(as.character(data[[i]]))
	# }
}

timeList <- hashmap(data[[timestamp]],rep(TRUE,nrow(data)))



print(head(data))


anomalies <- detect(data.frame(time = data[[timestamp]],value = data[[target]]), maxanoms, alpha, FALSE)
anomalies = na.omit(anomalies)
X <- c()
for(i in 1:nrow(anomalies)) {
	if(timeList$has_key(anomalies[i,1]) == FALSE) {
		X <- c(X,i)
	}
}
if(length(X) > 0) {
	print(X)
	anomalies <- anomalies[-X,]
}
S = paste("anomalies/","result_",target,"drilled.csv", sep="")
write.csv(anomalies, file=S,row.names=FALSE)
# anomalies <- read.csv(file = paste("anomalies/","result_",target,"drilled.csv", sep=""),header = TRUE, sep = ",")
# anomalies[["timestamp"]] <- as.POSIXct(strptime(anomalies[["timestamp"]], "%Y-%m-%d %H:%M:%S"))
# anomalies[["value"]] <- as.numeric(as.character(anomalies[["value"]]))

causes <- hashmap(anomalies[["timestamp"]],rep("",nrow(anomalies)))

# For categorical features
args[8] <- gsub("\r", "", args[8])
categorical <- strsplit(args[8], ",")
print(categorical[[1]])

for(cf in c(categorical[[1]])) {
	values <- unique(data[[cf]])
	print(values)
	for(val in values) {
		cur_frame <- data[data[[cf]]==val,c(timestamp,target)]
		colnames(cur_frame) <- c("time","value")
		cur_frame[["value"]] <- as.numeric(as.character(cur_frame[["value"]]))
		feature_anoms <- detect(cur_frame,maxanoms,alpha,TRUE,target)
		print(nrow(feature_anoms))
		times <- feature_anoms[["timestamp"]]
		
		for(d in 1:nrow(feature_anoms)) {
			k <- feature_anoms[d,1]
			v <- feature_anoms[d,2]
			if(causes$has_key(k)) {
				if(causes[[k]] == ""){
					causes[[k]] <- paste("At ",cf,"=",val,"NEWLINE",v,sep="")
				}
				else{
					causes[[k]] <- paste(causes[[k]],paste("At ",cf,"=",val,"NEWLINE",v,sep=""),sep="NEWLINE NEWLINE")
				}
			}
		}
	}
}

# For numeric features
args[9] <- gsub("\r", "", args[9])
numeric_features <- strsplit(args[9], ",")
for(nf in c(numeric_features[[1]])) {
	if(nf != target){
		cur_frame <- data[,c(timestamp,nf)]		
		colnames(cur_frame) <- c("time","value")
		cur_frame[["value"]] <- as.numeric(as.character(cur_frame[["value"]]))
		feature_anoms <- detect(cur_frame,maxanoms,alpha,TRUE,nf)
		times <- feature_anoms[["timestamp"]]
		for(d in 1:nrow(feature_anoms)) {
			k <- feature_anoms[d,1]
			v <- feature_anoms[d,2]
			if(causes$has_key(k)) {
				if(causes[[k]] == ""){
					causes[[k]] <- paste(nf," ",v,sep="")
				}
				else{
					causes[[k]] <- paste(causes[[k]],paste(nf," ",v,sep=""),sep="NEWLINE NEWLINE")
				}
			}
		}
	}
}
for(i in 1:nrow(anomalies)) {
	anomalies[i,c("causes")] <- gsub("\r","",causes[[anomalies[i,c("timestamp")]]])
}
write.csv(anomalies, file = paste("anomalies/causes_",filename,sep=""),row.names=FALSE)

data <- data[,c(timestamp,c(numeric_features[[1]]))]
feature <- colnames(data)
ind <- -1
for (i in 1:length(feature)) {
	if(feature[i] == timestamp) {
		ind <- i
	}
}

print(ind)
colnames(data)[ind] <- "time"
data <- ddply(data,.(time),colwise(sum))
colnames(data)[ind] <- feature[ind]

write.csv(data,file=paste("files/drilled.csv"),row.names=FALSE)

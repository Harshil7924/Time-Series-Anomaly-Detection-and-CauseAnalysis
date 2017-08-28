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
library(pryr)  
library(digest)  
mem_change(x <- 1:2000e6) 
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
	if(nrow(df)==0) return (data.frame(timestamp=as.POSIXct(character()),value=character(),delta=as.numeric(character())))
	df <- aggregate(value~time, sum, data=df)
	df1.zoo<-zoo(df[,-1],df[,1])
	df2 <- merge(df1.zoo,zoo(,seq(start(df1.zoo),end(df1.zoo),by=args[2])), all=TRUE)
	df <- data.frame(timestamp = index(df2), value = coredata(df2))
	if(nrow(df)<=5) return (data.frame(timestamp=as.POSIXct(character()),value=character(),delta=as.numeric(character())))
	#fill NAs
	df[[2]] <- na.fill(df[[2]],0)
	
	#trend = runmed(df[[2]], freq)

	trend <- numeric(length=nrow(df))
	ptr <-1
	piecewiseperiod = freq
	while(ptr <= nrow(df)) {
		end <- min(ptr+piecewiseperiod-1,nrow(df))
		trend[ptr:end] <- median(df[ptr:end,2])
		ptr <- ptr + piecewiseperiod
	}

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
	obs <- remove(df[[2]], add(trend,seasonal))

	n <- length(obs)
	r <- trunc(maxanoms*n)
	outlier_ind <- numeric(length=r) 
	m <- 0 
	obs_new <- obs 
	
	expected <- numeric(length=nrow(df))

	for(i in 1:r){
			
		MAD <- mad(obs_new)
		if(is.na(MAD))
			break
		if(MAD == 0)
			break
		md <- median(obs_new)
		if(is.na(md)) break
		z <- abs(obs_new - md)/MAD 
		
		max_ind <- which(z==max(z),arr.ind=T)[1] 
		R <- z[max_ind]
		outlier_ind[i] <- which(obs_new[max_ind] == obs, arr.ind=T)[1] 
		obs_new <- obs_new[-max_ind]
		p <- 1 - alpha/(2*(n-i+1)) 
		t_pv <- qt(p,df=(n-i-1)) 
		lambda <- ((n-i)*t_pv) / (sqrt((n-i-1+t_pv^2)*(n-i+1)))
		

		result = tryCatch({
			if(R > lambda) {
				m <- i
			}
		}, warning = function(w) {

		}, error = function(e) {

			print("error lamba")
			print(z)
			print(lambda)
			print(R)
			print(t_pv)
			print(n)

		}, finally = {

		})
		result = tryCatch({

			if(obs[outlier_ind[i]] >= MAD*lambda + md) {
				expected[outlier_ind[i]] <- trend[outlier_ind[i]] + seasonal[outlier_ind[i]] + MAD*lambda + md
			}
			else{
				expected[outlier_ind[i]] <- md - MAD*lambda + trend[outlier_ind[i]] + seasonal[outlier_ind[i]]
			}
			expected[outlier_ind[i]] <- abs(expected[outlier_ind[i]])
		}, warning = function(w) {

		}, error = function(e) {
			print("error in expected value")
			print(outlier_ind)
			print(outlier_ind[i])
			print(lambda)
			print(md)
			print(MAD)


		}, finally = {
		})
		
	}
	if(m == 0) {
		return (data.frame(timestamp=as.POSIXct(character()),value=character(),delta=as.numeric(character())))
	}
	outlier_ind <- outlier_ind[1:m]
	outlier_ind <- unique(outlier_ind)

	ret = df[outlier_ind,]
	j <- 1
	for( i in outlier_ind) {
		ret[j,c("delta")] <- df[i,2] - expected[i]
		j <- j+1
	}	
	if(getScore) {
		j <- 1
		for(i in outlier_ind) {
			if(length(trend[i])!=0 && length(seasonal[i])!=0 ) {
				score <- (df[i,2] - expected[i])/expected[i]
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
					temp <- paste("BOLDO",score,"%BOLDC",ch,"NEWLINE Actual ",metric," --> ",sprintf("%.2f",df[i,2]),"NEWLINE Expected ",metric," --> ",sprintf("%.2f",expected[i]),sep="")
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
	ret = ret[order(ret[[1]]),]
	return (ret)	
}

args <- commandArgs(TRUE)
#print(args)
filename <- args[1]

model <- args[3]
maxanoms <- as.numeric(as.character(args[4]))
alpha <- as.numeric(as.character(args[5]))
target <- args[6]
timestamp <- args[10]
tsformat <- args[11]

S<- args[1]
for(i in 2:11) {
	S <- paste(S,args[i],sep="")
}

mad5 <- digest(S,algo="md5", serialize=FALSE)


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
f <- file(paste("files/",filename,sep=""))
if(args[8]!="") {
	sqlQuery <- paste('Select ',timestamp,",",args[8],",",args[9],' from f ',sep="")
} else{
	sqlQuery <- paste('Select ',timestamp,",",args[9],' from f ',sep="")
}
if(!is.na(args[7]) && args[7]!="") {
	sqlQuery <- paste(sqlQuery,'where ',as.character(args[7]),sep="")
}

#print(sqlQuery)
data <- sqldf(sqlQuery, stringsAsFactors = FALSE,dbname = tempfile(), file.format =list(header = T, row.names = F))
#print("Dataread")
#print(head(data))

if(nrow(data) == 0) {
	S = paste("anomalies/","result_",target,mad5,"_",filename,sep="")
	write.csv(data.frame(timestamp=anomalies[["timestamp"]],value=anomalies[["value"]]), file=S,row.names=FALSE)
	write.csv(data.frame(timestamp=anomalies[["timestamp"]],value=anomalies[["value"]],causes=anomalies[["causes"]]), file = paste("anomalies/causes_",mad5,"_",filename,sep=""),row.names=FALSE)
	write.csv(data,file=paste("files/causesdrilled_",mad5,"_",filename,sep=""),row.names=FALSE)
	q()
}
if(nrow(data)>0){
#	print(tsformat)
	data[[timestamp]] <- as.POSIXct(strptime(data[[timestamp]], tsformat))
	data <- data[order(data[[timestamp]]),]
	# names <- colnames(data)
	# for(i in 1:ncol(data)) {
	# 	if(names[i] != timestamp)
	# 		data[[i]] <- as.numeric(as.character(data[[i]]))
	# }
}

timeList <- hashmap(data[[timestamp]],rep(TRUE,nrow(data)))


#print(head(data))


anomalies <- detect(data.frame(time = data[[timestamp]],value =as.numeric(as.character( data[[target]]))), maxanoms, alpha, FALSE)


X <- c()
for(i in 1:nrow(anomalies)) {
	if(timeList$has_key(anomalies[i,1]) == FALSE) {
		X <- c(X,i)
	}
}
if(length(X) > 0) {
	anomalies <- anomalies[-X,]
}
causes <- hashmap(anomalies[["timestamp"]],rep("",nrow(anomalies)))

if(nrow(anomalies)>0){
	for(i in 1:nrow(anomalies)) {
		k = anomalies[i,1]
		v = anomalies[i,2]
		s = anomalies[i,3]
		
		result = tryCatch({
			if(s>0) {
				ch <- "PLUS"
			} else {
				ch <- "MINUS"
			}
			score <- abs(s/(v-s))
			score <- score*100
			score <- sprintf("%.2f",score)
			txp <- v-s
			causes[[k]] <- paste("BOLDO",score,"%BOLDC",ch,"NEWLINE Actual ",target," --> ",sprintf("%.2f",v),"NEWLINE","Expected ",target," --> ",sprintf("%.2f",txp),sep="")
		}, warning = function(w) {

		}, error = function(e) {
			print(e)
		}, finally = {

		})
	}
}
S = paste("anomalies/","result_",target,mad5,"_",filename,sep="")
write.csv(data.frame(timestamp=anomalies[["timestamp"]],value=anomalies[["value"]]), file=S,row.names=FALSE)
# anomalies <- read.csv(file = paste("anomalies/","result_",target,"drilled.csv", sep=""),header = TRUE, sep = ",")
# anomalies[["timestamp"]] <- as.POSIXct(strptime(anomalies[["timestamp"]], "%Y-%m-%d %H:%M:%S"))
# anomalies[["value"]] <- as.numeric(as.character(anomalies[["value"]]))
#print("Anomalies calculated")
allDeltas <- hashmap(anomalies[["timestamp"]],anomalies[["delta"]])

# For categorical features
args[8] <- gsub("\r", "", args[8])
categorical <- strsplit(args[8], ",")
causesDf <- data.frame(timestamp=as.POSIXct(character()),cs=character(),delta=as.numeric(character()))

epoch <- 0

itrs <- NA

tot_ind <- c()
if(nrow(anomalies)!=0) {
	for(i in 1:nrow(anomalies)) {
		ind = which(data[[timestamp]]==anomalies[i,1])
		tot_ind <- c(tot_ind,ind)
	}
}
itrs <- data[tot_ind,c(categorical[[1]])]
for(cf in colnames(itrs)){
	values <- unique(itrs[[cf]])
	for(val in values) {
		#print(paste("epoch ",toString(epoch),cf,val))
		epoch <<- epoch+1
		cur_frame <- data[data[[cf]]==val,c(timestamp,target)]
		colnames(cur_frame) <- c("time","value")
		cur_frame[["value"]] <- as.numeric(as.character(cur_frame[["value"]]))
		feature_anoms <- detect(cur_frame,maxanoms,alpha,TRUE,target)
		times <- feature_anoms[["timestamp"]]
		
		for(d in 1:nrow(feature_anoms)) {
			k <- feature_anoms[d,1]
			v <- feature_anoms[d,2]
			s <- feature_anoms[d,3]
			if(allDeltas$has_key(k)) {
				

			result = tryCatch({

				if((abs(s/allDeltas[[k]]) >= 0.1) &  ((s*allDeltas[[k]])>0) ){
					causesDf <- rbind(causesDf,data.frame(timestamp=k,cs=paste("At ",cf,"=",val,"NEWLINE",v,sep=""),delta=abs(s)))
				}

			}, warning = function(w) {

			}, error = function(e) {
				print("error in rbind")
				print(s)
				print(allDeltas[[k]])
				print(k)
				print(v)
				print(cf)
				print(val)

			}, finally = {
			})
			
				
			}
		}
	}
}

target_med <- median(data[,c(target)])

# For numeric features
args[9] <- gsub("\r", "", args[9])
numeric_features <- strsplit(args[9], ",")
for(nf in c(numeric_features[[1]])) {
	if(nf != target){
		#print(paste("epoch ",toString(epoch),nf))
		epoch <<- epoch+1
		cur_frame <- data[,c(timestamp,nf)]		
		colnames(cur_frame) <- c("time","value")
		cur_frame[["value"]] <- as.numeric(as.character(cur_frame[["value"]]))
		feature_anoms <- detect(cur_frame,maxanoms,alpha,TRUE,nf)
		times <- feature_anoms[["timestamp"]]
		
		feature_med <- median(data[,c(nf)])
		scale_factor <- target_med/feature_med
		R <- cov(data[,c(target)],data[,c(nf)])
		for(d in 1:nrow(feature_anoms)) {
			k <- feature_anoms[d,1]
			v <- feature_anoms[d,2]
			s <- feature_anoms[d,3]


		result = tryCatch({
			s <- s*scale_factor
			if((abs(s/allDeltas[[k]]) >= 0.1) &  ((R*s*allDeltas[[k]])>0) )
				causesDf <- rbind(causesDf,data.frame(timestamp=k,cs=paste(nf," ",v,sep=""),delta=abs(s)))
				
			}, warning = function(w) {

		}, error = function(e) {
			print(e)
		}, finally = {

		})

		}
	}
}


causesDf = causesDf[order(-causesDf[["delta"]]),]
cnt <- hashmap(anomalies[["timestamp"]],rep(0,nrow(anomalies)))

if(nrow(causesDf)!=0){
	for(i in 1:nrow(causesDf)) {
		f <- causesDf[i,c("timestamp")]
		s <- causesDf[i,c("cs")]

		result = tryCatch({
			if(cnt[[f]] < 10){
				cnt[[f]] <- cnt[[f]] +1
				if(cnt[[f]] == 1) {
					causes[[f]] <- paste(causes[[f]],"NEWLINE NEWLINE BOLDO OPENU Causes: CLOSEU BOLDC",sep="")
				}
				causes[[f]] <- paste(causes[[f]],s,sep=" NEWLINE NEWLINE ")
			}
		}, warning = function(w) {

		}, error = function(e) {
			print(e)
		}, finally = {

		})




	}
	for(i in 1:nrow(anomalies)) {
		anomalies[i,c("causes")] <- gsub("\r","",causes[[anomalies[i,c("timestamp")]]])
	}
}
write.csv(data.frame(timestamp=anomalies[["timestamp"]],value=anomalies[["value"]],causes=anomalies[["causes"]]), file = paste("anomalies/causes_",mad5,"_",filename,sep=""),row.names=FALSE)

data <- data[,c(timestamp,c(numeric_features[[1]]))]
feature <- colnames(data)
ind <- -1
for (i in 1:length(feature)) {
	if(feature[i] == timestamp) {
		ind <- i
	}
}

colnames(data)[ind] <- "time"
data <- ddply(data,.(time),colwise(sum))
colnames(data)[ind] <- feature[ind]
write.csv(data,file=paste("files/causesdrilled_",mad5,"_",filename,sep=""),row.names=FALSE)
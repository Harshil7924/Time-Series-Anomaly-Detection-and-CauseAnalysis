# args[1] - filename to read
# args[2] - frequency of seasonality (Should be an odd number)
# args[3] - model name 'additive' | 'multiplicative'
# args[4] - max_anoms
# args[5] - alpha
# args[6] - comma separated column names
# args[7] - drill down options
library(sqldf)
freq <- 1441
model <- 'additive'
maxanoms <- 0.49
alpha <- 0.01


add <- function(A, B) {
	if(model == 'additive') {
		return (A+B)
	}
	return (A*B);
}

remove <- function(A,B) {
	if(model == 'additive') {
		return (A-B)
	}
	return (A/B);
}

detect <- function(df, maxanoms, alpha) {
	df <- na.omit(df)
	trend = runmed(df[[2]], freq)
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
	obs <- remove(df[[2]] , add(trend, seasonal))	

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
freq <- as.numeric(as.character(args[2]))
model <- args[3]
maxanoms <- as.numeric(as.character(args[4]))
alpha <- as.numeric(as.character(args[5]))

f <- file(paste("files/",filename,sep=""))
sqlQuery <- paste("Select ",args[6]," from f ")
if(!is.na(args[7]) && args[7]!="") {
	sqlQuery <- paste(sqlQuery,"where ",args[7])
}
print(sqlQuery)
data <- sqldf(sqlQuery, stringsAsFactors = FALSE,dbname = tempfile(), file.format =list(header = T, row.names = F))
if(nrow(data)>0)
	data[[1]] <- as.POSIXlt(strptime(data[[1]], "%Y-%m-%d %H:%M:%S"))
write.csv(data, file = paste("files/drilled.csv"),row.names=FALSE)
feature <- colnames(data)
F = ncol(data)


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
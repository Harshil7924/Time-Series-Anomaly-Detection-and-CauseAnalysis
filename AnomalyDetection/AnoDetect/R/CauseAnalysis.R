library(mvtnorm)
args <- commandArgs(TRUE)
filename <- args[1]
target <- args[2]
timestamp <- args[3]
theta <- as.numeric(as.character(args[4]))
anomalies = read.csv(file=paste("anomalies/","result_",target,filename, sep=""),head=TRUE,sep=',',stringsAsFactors=FALSE)
data = read.csv(file=paste("files/",filename,sep=""),head=TRUE,sep=',',stringsAsFactors=FALSE)

P = colnames(data)
for(i in 1:length(P)) {
	if(P[i] == target)
		data <- data[-i]
}

anomalies_index <- 1:nrow(anomalies)
for(i in 1:nrow(anomalies)) {
	anomalies_index[i] <- which(data[[timestamp]] == anomalies[i,1])[1]
}

P = colnames(data)
for(i in 1:length(P)) {
	if(P[i] == timestamp)
		data <- data[-i]
}

P = colnames(data)

len = length(P)
causes <- matrix(rep(FALSE,nrow(anomalies) * len), nrow=nrow(anomalies), ncol = len)

calc <- function(rm,c) {
	mu <- colMeans(rm)
	S <- cov(rm)
	for(i in 1:nrow(anomalies)) {
		cur <- dmvnorm(data[anomalies_index[i], c],mean=mu,sigma=S)
		cur <- as.numeric(cur)
		if(!is.na(cur) && cur < theta) {
			b <- TRUE
			for(j in c) {
				b <- (b&causes[i,j])
			}
			if(b == FALSE){
				for(j in c) {
					causes[i,j] <<- TRUE
				}	
			}
		}
	}
}

if(len!=0){
	for(i in 1:len) {
		rm <- NULL
		rm <- cbind(rm,data[[i]])
		calc(rm,c(i))
		if(i+1 <= len){	
			for(j in (i+1):len) {
				rm <- cbind(data[[i]], data[[j]])
				calc(rm,c(i,j))
			}
		}
	}
}

C <- rep("",nrow(anomalies))
if(len!=0){
	for(i in 1:nrow(anomalies)) {
		for(j in 1:length(P)) {
			if(causes[i,j] == TRUE) {
				C[i] <- paste(C[i], P[j], sep=" ")
			}
		}
	}
}
temp <- data.frame(timestamp = anomalies[[1]], value = anomalies[[2]], causes = C)

S = paste("anomalies/","causes_",filename, sep="")
write.csv(temp, file=S,row.names=FALSE)

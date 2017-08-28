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

mark <- matrix(rep(TRUE, nrow(anomalies)*(2^len)),nrow=nrow(anomalies), ncol = 2^len)
causes <- matrix(rep(FALSE,nrow(anomalies) * len), nrow=nrow(anomalies), ncol = len)

limit = 2^len-1
St <- 0
for (mask in 1:limit) {

	C <- logical(len)
	rm <- NULL
	for(i in 0:len-1) {
		if(bitwAnd(mask,2^i) != 0) {
			rm <- cbind(rm,data[[i+1]])
			C[i+1] <- TRUE
		}
		else{
			C[i+1] <- FALSE
		}
	}

	mu <- colMeans(rm)
	S <- cov(rm)

	for(i in 1:nrow(anomalies)) {

		if(mark[i,mask] == TRUE) {
			cur <- dmvnorm(data[anomalies_index[i], C],mean=mu,sigma=S)
			cur <- as.numeric(cur)
			if(cur < theta) {
				for(j in 0:len-1) {
					mark[i,bitwOr(mask,2^j)] <- FALSE
					if(bitwAnd(mask,2^j) !=0 ) {
						causes[i,j+1] <- TRUE
					}
				}	
			}
		}
	}
}
C <- rep("",nrow(anomalies))
for(i in 1:nrow(anomalies)) {
	for(j in 1:length(P)) {
		if(causes[i,j] == TRUE) {
			C[i] <- paste(C[i], P[j], sep=" ")
		}
	}
}
temp <- data.frame(timestamp = anomalies[[1]], value = anomalies[[2]], causes = C)

S = paste("anomalies/","causes_",filename, sep="")
write.csv(temp, file=S,row.names=FALSE)

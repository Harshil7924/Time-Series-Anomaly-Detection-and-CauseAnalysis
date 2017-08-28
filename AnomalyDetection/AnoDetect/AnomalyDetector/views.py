from django.shortcuts import render, redirect
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.http import HttpResponse
import rpy2.robjects as ro
import csv
import time
import subprocess
import os
import hashlib
# Create your views here.
def index(request):
    return HttpResponse("welcome")


def combineList(a, b):
    ret = []
    for i in range(0,len(a)):
        ret.append((a[i],b[i]))
    return ret

def simple_upload(request):
    fs = FileSystemStorage()
    files=[]
    for file in fs.listdir("files")[1][1:]:
        if 'drilled' not in file:
            f=open("files/"+file)
            files.append((file,os.stat('files/'+file).st_size,time.ctime(os.path.getmtime("files/"+file))))
    if request.method == 'POST':
        if 'myfile' in request.FILES:
            datafile = request.FILES['myfile']
            if fs.exists("files/"+datafile.name):
                return HttpResponse("File already exist")
            filename = fs.save("files/"+datafile.name, datafile)
            removeNull("files/"+datafile.name)
            datafile=datafile.name
            features=extractFeatures(filename)  
            uploaded_file_url = fs.url(filename)
            request.session["datafile"]=datafile
            request.session["filename"]=filename
            request.session["features"]=features
            return render(request, 'first.html', {
                'uploaded_file_url': uploaded_file_url,"featurenames": features, "files": files,"settings_tab":"active"
            })
        elif 'file1' in request.POST:
            datafile=request.POST['file1']
            filename="files/"+datafile
            features=extractFeatures(filename)
            request.session["datafile"]=datafile
            request.session["filename"]=filename
            request.session["features"]=features
            return render(request, 'first.html', {
                "featurenames": features, "files": files,"settings_tab":"active"})
        elif 'settings' in request.POST:
            features=request.session["features"]
            datafile=request.session["datafile"]
            num_features=[]
            for param in request.POST:
                if request.POST[param]=='on':
                    num_features.append(param)
            tsformat=request.POST['tsformat']
            timestamp=request.POST['timestamp']
            cat_features=(set(features)-set(num_features))
            cat_features.discard (timestamp)
            freq=request.POST['freq']
            model=request.POST['model']
            max_anoms=request.POST['max_anoms']
            alpha="0.05"
            params={"Model":model,"Frequency":freq,"Max_Anoms":max_anoms}
            request.session["model"]=model
            request.session["freq"]=freq
            request.session["max_anoms"]=max_anoms
            request.session["num_features"]=num_features
            request.session["timestamp"]=timestamp
            request.session["alpha"]=alpha
            request.session["cat_features"]=list(cat_features)
            request.session["tsformat"]=tsformat
            request.session["params"]=params

            return render(request, 'first.html', {
                 "num_features":num_features,"cat_features":cat_features, "files": files,"anomaly_tab":"active","params":params,"filename":datafile
            })
        
        elif 'ano' in request.POST:
            
            num_features=request.session["num_features"]
            cat_features=request.session["cat_features"]
            freq=request.session["freq"]
            model=request.session["model"]
            alpha=request.session["alpha"]
            timestamp=request.session["timestamp"]
            max_anoms=request.session["max_anoms"]
            datafile=request.session["datafile"]
            tsformat=request.session["tsformat"]
            params=request.session["params"]
            plot_features=[]
            for f in request.POST:
                if request.POST[f]=='on':
                    plot_features.append(f)
            
            r=request.POST['filterlist'].strip(".").replace(".",",").replace("\r\n","")
            flist=r.split(",")
            if '' in flist:
                flist.remove('')

            res=makecond(flist)
            

            fat=myanomalyDet(datafile,freq,model,max_anoms,alpha,timestamp,res,num_features,tsformat)
            if fat:
                return HttpResponse("Invalid Input")
            m = hashlib.md5()
            m.update(datafile+freq+model+str(max_anoms)+str(alpha)+timestamp+','+','.join(num_features)+res+tsformat+timestamp)
            
            print datafile+freq+model+str(max_anoms)+str(alpha)+timestamp+','+','.join(num_features)+res+tsformat+timestamp
            print m.hexdigest()

            Dict=csvToDict("files/anodrilled_"+m.hexdigest()+"_"+datafile,timestamp)
            anodict=csvToList("anomalies/result_",m.hexdigest()+"_"+datafile,num_features)
            
            retDict = {}
            for feature in num_features:
                 retDict[feature] = combineList(Dict[timestamp], Dict[feature])
            
            return render(request, 'first.html', {
                "plotData":retDict, "num_features":num_features, "cat_features":cat_features,"anomalies":anodict[0], "numanoms":anodict[1], "files": files,"anomaly_tab":"active",'plot_features':plot_features,"tsformat":tsformat,"filterlist":flist,"params":params,"filename":datafile
            })

        elif 'causes' in request.POST:
            num_features=request.session["num_features"]
            cat_features=request.session["cat_features"]
            freq=request.session["freq"]
            model=request.session["model"]
            alpha=request.session["alpha"]
            timestamp=request.session["timestamp"]
            max_anoms=request.session["max_anoms"]
            datafile=request.session["datafile"]
            tsformat=request.session["tsformat"]
            params=request.session["params"]

            print request.POST['cfilterlist']
            r=request.POST['cfilterlist'].strip(".").replace("."," and ").replace("\r\n","")
            cflist=r.split(" and ")
            if '' in cflist:
                cflist.remove('')

            res=makecond(cflist)
            ctarget=request.POST['ctarget']
            
            causes,fat=causeanalysis(datafile,timestamp,res,ctarget,max_anoms,alpha,rem(cat_features,r),num_features,freq,model,tsformat)
            if fat:
                return HttpResponse("Invalid Input")
            m = hashlib.md5()
            allparams=datafile+freq+model+max_anoms+str(alpha)+ctarget+res+','.join(rem(cat_features,r))+','.join(num_features)+timestamp+tsformat
            m.update(allparams)
            Dict=csvToDict("files/causesdrilled_"+m.hexdigest()+"_"+datafile,timestamp)
            anodict=csvToList("anomalies/result_",m.hexdigest()+"_"+datafile,[ctarget])
            
            retDict = {}
            for feature in num_features:
                 retDict[feature] = combineList(Dict[timestamp], Dict[feature])
            
            return render(request, 'first.html', {
                "plotData":retDict, "num_features":num_features, "cat_features":cat_features, "anomalies":anodict[0], "numanoms":anodict[1], "files": files, "causes":causes, "causes_tab": "active","target": ctarget,"params":params,"cfilterlist":cflist,"filename":datafile
            })
        else:
            return HttpResponse("Please Choose a file first")
           
    return render(request, 'first.html',{"files":files,"home_tab":"active"})

def makecond(flist):
    print flist
    D={}
    for f in flist:
        x=f.split('=')
        if x[0] in D:
            D[x[0]].append(x[1])
        else:
            D[x[0]]=[x[1]]
    res=""

    for k,d in D.items():
        res+="("
        for i in range(len(d)-1):
            res+=k+"="+d[i]+" or "
        res+=k+"="+d[-1]+") "
        res+=" and "
    res+="1 = 1"
    return res

def removeNull(filename):
    print "sed 's/NULL//g' -i " +filename
    subprocess.call(["sed"]+['-i','s/NULL//g' ,filename])

def findcauses(filename,allparams):
    m = hashlib.md5()
    m.update(allparams)
    print m.hexdigest()
    f=open("anomalies/causes_"+m.hexdigest()+"_"+filename)
    f.readline()
    lines=f.readlines()
    res={}
    for line in lines:
        l=line.replace("\"","").rstrip("\n").split(',')
        res[str(convertToInt(l[0]))+l[1]]=l[2]
    return res


def myanomalyDet(filename,freq,model,max_anoms,alpha,timestamp,r,num_features,tsformat):
    hashstring=filename+freq+model+str(max_anoms)+str(alpha)+timestamp+','+','.join(num_features)+r+tsformat+timestamp
    m=hashlib.md5()
    m.update(hashstring)
    fs = FileSystemStorage()
    for file in fs.listdir("files")[1][1:]:
        if m.hexdigest() in file:
            return False
    try:
        subprocess.check_output(['Rscript','R/AnoDet.R',filename,freq,model,str(max_anoms),str(alpha),timestamp+','+','.join(num_features),r,tsformat,timestamp], universal_newlines=False)
    except:
        return True
    return False

    
def rem(l,s):
    if s=='':
        return l
    t=list(l)
    sub=s.split(" and ")
    for x in sub:
        if(x.split("=")[0].strip(" ") in t):
            t.remove(x.split("=")[0].strip(" "))
    return t

def causeanalysis(filename,timestamp,r,ctarget,max_anoms,alpha,cat_features,num_features,freq,model,tsformat):
    hashstring=filename+freq+model+max_anoms+str(alpha)+ctarget+r+','.join(cat_features)+','.join(num_features)+timestamp+tsformat
    m=hashlib.md5()
    m.update(hashstring)
    fs = FileSystemStorage()
    for file in fs.listdir("files")[1][1:]:
        if m.hexdigest() in file:
            return findcauses(filename,hashstring),False
    try:
        subprocess.check_output(['Rscript','R/MCauseAnalysis.R',filename,freq,model,max_anoms,alpha,ctarget,r,','.join(cat_features),','.join(num_features),timestamp,tsformat], universal_newlines=False)
    except:
        return [],True
    return findcauses(filename,hashstring),False

def convertToInt(s):
    s=s.replace("\"","")
    return s

def csvToDict(filename,timestamp):
    Dict={}
    f=open(filename)
    cur_features=f.readline().strip('\n').replace("\"","").split(',')
    lines=f.readlines()
    for f in cur_features:
        Dict[f] = []
    for line in lines:
        l=line.strip("\n").split(',')
        for i in range(len(l)):
            if l[i] == timestamp:
                Dict[cur_features[i]].append(convertToInt(l[i]))
            else:
                Dict[cur_features[i]].append(l[i])
    return Dict
                
def csvToList(filename,hash, given_features):
    D={}
    numanoms={}
    for feature in given_features:
        lines=csv.reader(open(filename+feature+hash))
        l=[]
        flag=True
        for line in lines:
            if not flag:
                k,v=line
                k=convertToInt(k)
                l.append((k,v))
            flag=False
        D[feature]=l;
        numanoms[feature]=len(l)
    return D,numanoms

def extractFeatures(filename):
    l=open(filename).readline()
    l=l.strip("\n\r").replace("\"","").split(',');
    res=[]
    for el in l:
        res.append(el)
    return res




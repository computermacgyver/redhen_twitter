import glob
#import cld2 #TODO: Need to finish this and include in output.
import gzip
import json
import sys
import re
import multiprocessing
import datetime

#TODO: Anyting else to include in summary file?
#see https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/intro-to-tweet-json
OUTPUT_STR="|".join(["{created_at}","{id}","{tweet_lang}",
	"{user_id}","{user_created_at}", "{user_lang}", "{location}", "{statuses_count}",
	"{time_zone}", "{utc_offset}", #"{followers_count}", "{friends_count}",
	#"{cld_reliable}", "{cld_bytes}", "{cld_lang1}", "{cld_lang1_percent}", "{cld_lang2}", "{cld_lang2_percent}"
	"{text}"
	])+"\n"

#TODO: Decide final format.
# * Use "|" to separate fields.
# * Should text fields be enclosed in quotation marks?
# * Or, should "|" be replaced / escaped in text?

RE_CR_LF=re.compile(r"[\r\n]")
RE_MENTION=re.compile(r"@[a-zA-Z0-9_]+")
RE_URL=re.compile(r"https?://\S+")

def summarize_file(infile):
	#TODO: Use gzip compression or not on output?
	#outfile=infile.replace(".json.gz",".twt")
	outfile=infile.replace(".json.gz",".twt.gz")
	with gzip.open(outfile,"wt") as fhOut:
		with gzip.open(infile,"rt") as fh:
			for line in fh:
				vals=summarize_tweet(line)
				if vals!=None:
					fhOut.write(OUTPUT_STR.format(**vals))

def summarize_tweet(rawtweet):
	if rawtweet==None or rawtweet.strip()=="":
		return None
	
	try:
		tweet=json.loads(rawtweet)
	except Exception as e:
		sys.stderr.write("Failed to parse JSON: ")
		sys.stderr.write(str(e))
		sys.stderr.write("\n")
		sys.stderr.write(rawtweet)
		sys.stderr.write("\n")
		return None
	
	if not "id" in tweet:
		return None
		
	vals={}
	
	try:
		vals["id"]=tweet["id_str"]
		#Format tiemstamps to match TV format e.g., 20181211000034.967 (YYYYmmddHHMMSS.000)
		#Tweets to do not have units more precise than the second (omit .000)
		created_at=tweet["created_at"].replace(" +0000 "," ")
		created_at=datetime.datetime.strptime(created_at,"%a %b %d %H:%M:%S %Y")
		created_at=created_at.strftime("%Y%m%d%H%M%S")
		vals["created_at"]=created_at
		vals["tweet_lang"]=tweet["lang"] if "lang" in tweet else "NA"

		
		user=tweet["user"]
		vals["user_id"]=user["id_str"]
		vals["user_created_at"]=user["created_at"]
		vals["followers_count"]=user["followers_count"]
		vals["friends_count"]=user["friends_count"]
		vals["user_lang"]=user["lang"]

		if user["location"]!=None:
			#TODO: How to handel | in text?
			vals["location"]=RE_CR_LF.sub(" ",user["location"]).replace("|","\\|")
		else:
			vals["location"]="NA"

		vals["statuses_count"]=user["statuses_count"]
		vals["time_zone"]=user["time_zone"]
		vals["utc_offset"]=user["utc_offset"]


		if "extended_tweet" in tweet and "full_text" in tweet["extended_tweet"]:
			txt=tweet["extended_tweet"]["full_text"]
		else:
			txt=tweet["text"]

		#TODO: detect_tweet_lang(txt)
		#TODO: How to handel | in text?
		vals["text"]=RE_CR_LF.sub(" ",txt).replace("|","\\|")

		return vals
	except Exception as e:
		sys.stderr.write("Failed to extract attribute: ")
		sys.stderr.write(str(e))
		sys.stderr.write("\n")
	return None


#TODO: Finish CLD2 language detection
def detect_tweet_lang(text):
	try:
		#Remove mentions and URLs before trying to detect language
		text=RE_MENTION.sub(" ",text)
		text=RE_URL.sub(" ",text)
		#
		#vals["cld_reliable"], vals["cld_bytes"], details = cld2.detect(text.encode("UTF-8"))
		#if len(details)>1:
		#	vals["cld_lang1"]=details[0][1]
		#	vals["cld_lang1_percent"]=details[0][2]
		#if len(details)>2:
		#	vals["cld_lang2"]=details[1][1]
		#	vals["cld_lang2_percent"]=details[1][2]
	except Exception as e2:
		sys.stderr.write("CLD error: ")
		sys.stderr.write(str(e2))
		sys.stderr.write("\n")

if __name__=="__main__":
	files=glob.glob("../tmp/*.json.gz")
	#TODO: Multiprocess on final pass
	#with multiprocessing.Pool(1) as pool:
	#	pool.imap_unordered(summarize_file,files)
	for file in files:
		summarize_file(file)
		#TODO: Move original and summary file to proper location in directory tree and rename, e.g., 2018-12-11_0000_WW_Twitter_Spritzer.twt
		#TODO: Is everyone happy with "WW" for "world-wide" / non-geographic data?

#!/usr/local/bin/python3
import sys
import os
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import arrow
import re
import smtplib
import zipfile
import email.utils
import email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from os.path import basename
import email.mime.application

src_table_nm = sys.argv[1]
#tgt_table_nm = sys.argv[2]
#param_nm = sys.argv[3]

dt=arrow.now().format('YYYYMMDD')
dtMS=arrow.now().format('HHMS')

archive_file= """/home/odmbatch/odm/archive/"""+dtMS+"""_ODM_VALIDATION_"""+src_table_nm+"""_"""+dt+""".csv"""

validation_file="""/home/odmbatch/odm/data/"""+dtMS+"""_ODM_VALIDATION_"""+src_table_nm+"""_"""+dt+""".csv"""
Path(validation_file).touch()

dty=arrow.now().format('YYYY-MM-DD')
dt=arrow.now().format('YYYYMMDD')
f="""ODM_VALIDATION_"""+src_table_nm+"""_"""+dt+""".csv"""
archive_folder="""/home/odmbatch/odm/archive/"""
archive_file="""/home/odmbatch/odm/archive/ODM_VALIDATION_"""+src_table_nm+"""_"""+dt+""".zip"""

fantasy_zip = zipfile.ZipFile(archive_file, 'w')
 
for folder, subfolders, files in os.walk(archive_folder):
 
    for file in files:
        if file.endswith('.csv'):
            fantasy_zip.write(os.path.join(folder, file),os.path.relpath(os.path.join(folder,file),archive_folder),compress_type = zipfile.ZIP_DEFLATED)
 
fantasy_zip.close()

 
#html to include in the body section
html = """Hi Team, 
"""+"""\n"""+"""
This is zipped file report for """+src_table_nm+""" on """+dt
 
# Creating message.
msg = MIMEMultipart('alternative')
msg['Subject'] = """Zip file report """+src_table_nm+""" on """+dt
msg['From'] = "Praveen_Kshirsagaray@eogresources.com"
msg['To'] = "Praveen_Kshirsagaray@eogresources.com,Diane_Ortiz@eogresources.com,Radhika_Tati@eogresources.com"
 
# The MIME types for text/html
HTML_Contents = MIMEText(html, 'html')
 
# Adding pptx file attachment
filename=archive_file
fo=open(filename,'rb')
attach = email.mime.application.MIMEApplication(fo.read(),_subtype="zip")
fo.close()
attach.add_header('Content-Disposition','attachment',filename=filename)
 
# Attachment and HTML to body message.
msg.attach(attach)
msg.attach(HTML_Contents)
 
 
# Your SMTP server information
s_information = smtplib.SMTP()

s_information.connect('smtp.eogresources.com')

s_information.sendmail(msg['From'], msg['To'], msg.as_string())
s_information.quit()


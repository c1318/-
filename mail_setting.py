import smtplib
from email.mime.application import MIMEApplication

def mail_login():
    smtpserver = "smtp.exmail.qq.com"
    username = "xxsjfxyj@jycinema.com"
    password = "JYshuju666"
    sender = "xxsjfxyj@jycinema.com" 
    smtp = smtplib.SMTP_SSL(smtpserver,465)
    smtp.ehlo()
    smtp.login(user = username,password = password)
    return smtp,sender

def attach_xlsx(xlsx_name,msg):
    att = MIMEApplication(open(xlsx_name,"rb").read())
    att.add_header("Content-Disposition","attachment",filename = ("GBK","",xlsx_name))
    msg.attach(att)
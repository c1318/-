import smtplib
from os import remove
from logger import logger
from database import read_sql
from mail_setting import mail_login,attach_xlsx
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr


#发送邮箱
def send_mail(file_name):
    #邮箱列表，需要处理成发送需要的字符串格式
    mail_addr_sql = "select mail_addr from mail_addr"
    mail_addr_list = read_sql(mail_addr_sql)["mail_addr"].tolist()
    mail_addr_str = ""
    for each_addr in mail_addr_list:
        mail_addr_str += each_addr + ","
    
    #登录邮箱
    smtp,sender = mail_login()
    receiver = mail_addr_str.rstrip(",")
    
    #编辑邮件内容
    msg = MIMEMultipart()
    msg["Subject"] = file_name.rstrip(".xlsx")
    msg["From"] = formataddr(["信息数据分析研究中心",sender])
    msg["To"] = receiver
    
    #添加主题和附件
    text = MIMEText(file_name.rstrip(".xlsx"))
    msg.attach(text)
    attach_xlsx(file_name,msg)
    #执行邮件发送
    try:
        smtp.sendmail(sender,receiver.split(","),msg.as_string())
        smtp.quit()
        logger.info("send mail success")
    except smtplib.SMTPException as e:
        logger.info("send mail failed,causing details : %s" % e)
    
    #发送后清理文件
    remove(file_name)
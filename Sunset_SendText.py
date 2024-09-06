# Databricks notebook source
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import dotenv_values
import os
import sys

# COMMAND ----------

secrets = dotenv_values("/Workspace/Users/jordan@jordanmartinetti.com/.env")

# COMMAND ----------

email = secrets['EMAIL']
password = secrets['EMAIL_PASSWORD']
recipient = secrets['RECIPIENT']

# COMMAND ----------

msg = MIMEMultipart()
msg["Subject"] = "Chicken Alarm"
msg["From"] = email
msg["To"] = recipient

# COMMAND ----------

message = "Lock up the chickens"
msg.attach(MIMEText(message))

# COMMAND ----------

msg.as_string()

# COMMAND ----------

print("Setting server")
server = smtplib.SMTP("smtp.gmail.com", 587)
print("Sending hello")
server.ehlo()
# Check the response to EHLO
if server.ehlo_resp:
    print("EHLO response is positive.")
else:
    print("EHLO response is not positive, check connection and try again.")
    sys.exit()

# Proceed with STARTTLS to secure the connection
server.starttls()
# After STARTTLS, it's a good practice to call ehlo() again
server.ehlo()

# Now you can proceed with login and sending the email
server.login(email, password)
text = msg.as_string()
server.sendmail(email, recipient, text)
server.quit()

# COMMAND ----------

print("Message sent, script ending")

# COMMAND ----------



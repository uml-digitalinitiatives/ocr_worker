## Description
This is a Python script to handle long-running OCR/hOCR tasks from an Islandora site. It replaces the
Hypercube microservice and is designed to be run as a standalone daemon. 

It connects in place of Alpaca (i.e you should disable the ocr derivative service from your Alpaca configuration).

It connects to your ActiveMQ/Artemis message broker, listens for OCR task messages, processes them by calling tesseract
with the provided arguments. It then uploads the resulting OCR/hOCR files directly back to your Islandora Drupal site 
skipping Alpaca). 

This script requires Python 3.x and the Python packages specified in `requirements.txt`.

I wrote this as some of our images required a long time to process with tesseract and the task was causing some messages to time out while waiting for the response. This
script will NACK messages it can't process so they are not lost and only ACK once the OCR/hOCR files are successfully uploaded back to Islandora.




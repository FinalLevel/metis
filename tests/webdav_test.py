"""
 Copyright (c) 2014 Final Level
 Author: Denys Misko <gdraal@gmail.com>
 Distributed under BSD (3-Clause) License (See
 accompanying file LICENSE)

 Description: Metis WebDAV implementation tests
"""

from tinydav import WebDAVClient
import binascii
import random
import time
import os

def gen_file(filename, size):
  """
    Generates random data filled file
  """
  try:
    fd = open(filename, "w+")
    data = ""
    for i in range(1, size):
      data += str(int('0') + (i % 25))
    fd.write(data)
    fd.seek(0)
    crc = binascii.crc32(data)
    return (fd, crc)
  except IOError as e:
     print "I/O error({0}): {1}".format(e.errno, e.strerror)
     return None


def upload_file(client, fileobject, uri):
  """
    Uploads file to WebDav server
  """
  response = client.put(uri, fileobject)
  if response == 201:
    return True
  else:
    return False

def download_file(client, uri, crc):
  """
    Uploads file to WebDav server
  """
  response = client.get(uri)
  if response == 200:
    response_crc = binascii.crc32(response.content)
    if response_crc == crc:
      return True
    else:
      return False
  else:
    return False

if __name__ == "__main__":
  random.seed(time.time())
  good_put = 0
  error_put = 0
  good_get = 0
  error_get = 0
  total_size = 0
  for c in range(1, 10):
    client = WebDAVClient("192.168.56.103", 6601)
    for r in range(1, 10):
      filename = "/tmp/" + "metis_web_dav_test_" + str(r)
      size = random.randint(1024, 100000)
      (fileobject, crc) = gen_file(filename, size)
      uri = "/1/2/" + str(r) + ".jpg"
      res = upload_file(client, fileobject, uri)
      if res:
        good_put += 1
      else:
        error_put += 1
      res = download_file(client, uri, crc)
      if res:
        good_get += 1
      else:
        error_get += 1
      total_size += size
      fileobject.close()
      os.unlink(filename)

  print "good_put=" + str(good_put) + ",error_put=" + str(error_put) + ",good_get=" + str(good_get) + ",error_get=" \
        + str(error_get) + ",total_size=" + str(total_size)


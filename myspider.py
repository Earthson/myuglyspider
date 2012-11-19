#!/usr/bin/env python

from io import BytesIO
from gzip import GzipFile
import urllib.request as request
from urllib.request import Request, urlopen
import chardet

import re
from threading import Thread
from queue import Queue
from time import sleep

proxy_support = request.ProxyHandler({'http':'http://localhost:9999'})
opener = request.build_opener(proxy_support, request.HTTPHandler)
request.install_opener(opener)

url_queue = Queue()

def read_urls(filename):
    ff = open(filename, 'r')
    for each in ff:
        url_queue.put(each)
    ff.close()



import zlib

def un_deflate(data):   # zlib only provides the zlib compress format, not the deflate format;
    try:               # so on top of all there's this workaround:
        return zlib.decompress(data, -zlib.MAX_WBITS)
    except zlib.error:
        return zlib.decompress(data)

def un_gzip(data):
    return GzipFile(fileobj=BytesIO(data), mode='r').read()

headers = {
    'Accept-Encoding' : 'gzip',
    'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
}

def get_request(url, headers=headers):
    req = Request(url, headers=headers)
    return req, req.get_host()

def get_html(req):
    response = urlopen(req, timeout=60)
    html = response.read()
    if response.headers.get('content-encoding') == 'gzip':
        html = un_gzip(html)
    #elif response.headers.get('content-encoding') == 'deflate':
    #    html = un_deflate(html)
    encoding = chardet.detect(html)['encoding']
    tmp = re.search(r'^http://([^/]+)', response.geturl())
    reqhost = tmp.group(1) if tmp else None
    info = response.info()
    info = dict(info)
    response.close()
    c_type = info['Content-Type'].split(';')[0]
    if(c_type == 'text/html'):
        html = str(html, encoding)
    else:
        html = None
    return html, reqhost

def pattern_gen(pattern, repl):
    pattern = re.compile(pattern)
    return lambda text: pattern.sub(repl, text)

def tag_pattern_gen(tag, repl=' '):
    pattern = re.compile(r'<%s[^>]*>[\s\S]*?</%s>' % (tag, tag))
    return lambda text: pattern.sub(repl, text)

def tag_gen(tag):
    pattern = re.compile(r'<%s[^>]*>([\s\S]+)</%s>' % (tag, tag))
    def get_tag(text):
        mobj = pattern.search(text)
        if mobj is None:
            return ' '
        return ' ' + mobj.group(1) + ' '
    return get_tag

def url_gen():
    pattern = re.compile(r'<a href="([^"]+)"[^>]*>([^<]*)</a>')
    def get_url(text, reqhost):
        mobjs = pattern.finditer(text)
        for mobj in mobjs:
            tmp = mobj.group(1)
            if tmp[0] == '#':
                continue
            if tmp[0] == '/':
                tmp = 'http://' + reqhost + tmp
            url_queue.put(tmp)
    return get_url

get_url = url_gen()

clear_script = tag_pattern_gen('script', repl=' ')
clear_style = tag_pattern_gen('style', repl=' ')
get_title = tag_gen('title')
get_body = tag_gen('body')
clear_tag = pattern_gen(r'<[^>]+>', ' ')
merge_blank = pattern_gen(r'[\s]+', ' ')

def translate_html(html, reqhost):
    
    try:
        title = get_title(html)
    except:
        return None, None
    body = get_body(html)
    body = clear_style(body)
    body = clear_script(body)
    if len(body) < 100:
        return None, None
    get_url(html, reqhost) #get new urls
    #title = clear_tag(title)
    body = clear_tag(body)
    #title = merge_blank(title)
    body = body.replace('\n', '')
    body = merge_blank(body)
    return title, body

import sys

def url_mapper(url):
    ohost = ''
    try:
        req, ohost = get_request(url)
        html, reqhost = get_html(req)
    except Exception as e:
        print('### %s' % url, file=sys.stderr)
        print(e, file=sys.stderr)
        return None, None
    return translate_html(html, reqhost)

from emmongodict import EmMongoDict

class DocFromWeb(EmMongoDict):
    db_info = {
        'db' : 'MachineLearningDB',
        'collection' : 'DocOnWeb',
    }
    indexes = {
        'url' : {'unique':True},
    }
    
DocFromWeb.init_collection()
DocFromWeb.ensure_index()

cnt = 0
def working():
    while True:
        global cnt
        url = url_queue.get()
        tmp = DocFromWeb(spec={'url':url})
        if not tmp.is_exist():
            title, body = url_mapper(url)
            if body is not None:
                DocFromWeb(doc={'url':url, 'title':title, 'body':body})
                cnt += 1
                ttt = str(cnt)
                ll = 8 - len(ttt)
                print(ttt+' '*ll, '#',  url)
        if cnt > 20000:
            return
        #sleep(30)


if __name__ == '__main__':
    read_urls('initurls.conf')
    
    NUM = 30
    for i in range(NUM):
        t = Thread(target=working)
        t.setDaemon(True)
        t.start()
        #sleep(0.01)
    while True:
        sleep(30)

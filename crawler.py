#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests
import re
import yaml
import base64
import json
import socket
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import time
import hashlib

# ======================== 全局配置 ========================
MAX_DEPTH = 3
MAX_REQUESTS = 300
REQUEST_TIMEOUT = 30
KEYWORDS = ['subscri', 'feed', '.yaml', '.yml', '.txt']
OUTPUT_YAML = 'crawclash.yaml'
OUTPUT_TXT = 'crawsub.txt'
SOURCE_FILE = 'crawler.list'
CONNECT_TIMEOUT = 3
TEST_WORKERS = 20

# ======================== 辅助函数 ========================

def clean_url(url):
    """去除 URL 末尾常见的标点符号"""
    if not url:
        return url
    return re.sub(r'[:,.?!;]+$', '', url.strip())

def safe_urljoin(base, url):
    """安全的 urljoin"""
    if not url:
        return None
    try:
        return urljoin(base, url)
    except:
        return None

def is_node_link(url):
    """判断 URL 是否可能是节点订阅链接"""
    if not url:
        return False
    lower = url.lower()
    if lower.endswith(('.yaml', '.yml', '.txt')):
        return True
    for kw in KEYWORDS:
        if kw.lower() in lower:
            return True
    return False

def fetch_content(url, retries=3, delay=1):
    """获取网页文本内容"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    }
    for i in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            if resp.encoding is None:
                resp.encoding = 'utf-8'
            return resp.text
        except Exception as e:
            if i < retries - 1:
                time.sleep(delay)
                continue
            raise
    return None

def fetch_binary(url, retries=3, delay=1):
    """获取二进制内容"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    for i in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            if i < retries - 1:
                time.sleep(delay)
                continue
            raise
    return None

# ----- 节点解析核心 -----

def parse_vmess(vmess_url):
    if not vmess_url.startswith('vmess://'):
        return None
    try:
        b64 = vmess_url[8:]
        b64 += '=' * (4 - len(b64) % 4)
        decoded = base64.b64decode(b64).decode('utf-8')
        data = json.loads(decoded)
        node = {
            'type': 'vmess',
            'add': data.get('add', ''),
            'port': int(data.get('port', 0)),
            'id': data.get('id', ''),
            'aid': int(data.get('aid', 0)),
            'net': data.get('net', 'tcp'),
            'type': data.get('type', 'none'),
            'host': data.get('host', ''),
            'path': data.get('path', ''),
            'tls': data.get('tls', ''),
            'sni': data.get('sni', ''),
        }
        return node
    except:
        return None

def parse_ss(ss_url):
    if not ss_url.startswith('ss://'):
        return None
    try:
        content = ss_url[5:]
        if '@' in content:
            prefix, suffix = content.split('@', 1)
            b64 = prefix.replace('-', '+').replace('_', '/')
            b64 += '=' * (4 - len(b64) % 4)
            decoded = base64.b64decode(b64).decode('utf-8')
            method, password = decoded.split(':', 1)
            host, port = suffix.split(':')
            port = int(port)
        else:
            b64 = content.replace('-', '+').replace('_', '/')
            b64 += '=' * (4 - len(b64) % 4)
            decoded = base64.b64decode(b64).decode('utf-8')
            method, password, host, port = re.split(r'[:@]', decoded)
            port = int(port)
        node = {
            'type': 'ss',
            'add': host,
            'port': port,
            'method': method,
            'password': password,
        }
        return node
    except:
        return None

def parse_trojan(trojan_url):
    if not trojan_url.startswith('trojan://'):
        return None
    try:
        parsed = urlparse(trojan_url)
        password = parsed.username or ''
        host = parsed.hostname or ''
        port = parsed.port or 443
        node = {
            'type': 'trojan',
            'add': host,
            'port': port,
            'password': password,
            'sni': parsed.hostname,
            'allowInsecure': parse_qs(parsed.query).get('allowInsecure', ['0'])[0],
        }
        return node
    except:
        return None

def parse_node_link(link):
    if link.startswith('vmess://'):
        return parse_vmess(link)
    elif link.startswith('ss://'):
        return parse_ss(link)
    elif link.startswith('trojan://'):
        return parse_trojan(link)
    else:
        return None

def parse_subscription_content(content, url_hint=''):
    nodes = []
    if not content:
        return nodes

    # 尝试 Clash YAML
    try:
        data = yaml.safe_load(content)
        if isinstance(data, dict):
            # 处理常见的 Clash 结构
            proxy_list = data.get('proxies') or data.get('Proxy') or data.get('proxy') or []
            if proxy_list:
                for proxy in proxy_list:
                    node = {
                        'type': proxy.get('type', ''),
                        'add': proxy.get('server', ''),
                        'port': int(proxy.get('port', 0)),
                        'uuid': proxy.get('uuid', ''),
                        'aid': proxy.get('alterId', 0),
                        'cipher': proxy.get('cipher', ''),
                        'net': proxy.get('network', 'tcp'),
                        'tls': proxy.get('tls', False),
                        'sni': proxy.get('sni', ''),
                        'host': proxy.get('host', ''),
                        'path': proxy.get('path', ''),
                        'raw': f"{proxy.get('type', '')}://{proxy.get('server', '')}:{proxy.get('port', '')}"
                    }
                    nodes.append(node)
                return nodes
    except:
        pass

    # 尝试 Base64
    try:
        b64_clean = content.replace('\n', '').replace('\r', '').replace(' ', '')
        if re.match(r'^[A-Za-z0-9+/=]+$', b64_clean) and len(b64_clean) % 4 == 0:
            decoded = base64.b64decode(b64_clean).decode('utf-8', errors='ignore')
            for line in decoded.splitlines():
                line = line.strip()
                if line and (line.startswith(('vmess://', 'ss://', 'trojan://'))):
                    node = parse_node_link(line)
                    if node:
                        node['raw'] = line
                        nodes.append(node)
            if nodes:
                return nodes
    except:
        pass

    # 普通文本，每行一个链接
    for line in content.splitlines():
        line = line.strip()
        if line and (line.startswith(('vmess://', 'ss://', 'trojan://'))):
            node = parse_node_link(line)
            if node:
                node['raw'] = line
                nodes.append(node)

    return nodes

def normalize_node_key(node):
    if node.get('type') == 'vmess':
        key = f"vmess_{node.get('add')}_{node.get('port')}_{node.get('id')}"
    elif node.get('type') == 'ss':
        key = f"ss_{node.get('add')}_{node.get('port')}_{node.get('method')}_{node.get('password')}"
    elif node.get('type') == 'trojan':
        key = f"trojan_{node.get('add')}_{node.get('port')}_{node.get('password')}"
    else:
        key = f"{node.get('type')}_{node.get('add')}_{node.get('port')}"
    return hashlib.md5(key.encode()).hexdigest()

def test_node_connectivity(node):
    host = node.get('add', '')
    port = node.get('port', 0)
    if not host or not port:
        return False
    try:
        ip = socket.gethostbyname(host)
        with socket.create_connection((ip, port), timeout=CONNECT_TIMEOUT):
            return True
    except:
        return False

def node_to_vmess_link(node):
    if node.get('type') == 'vmess':
        if node.get('raw') and node['raw'].startswith('vmess://'):
            return node['raw']
        data = {
            'v': '2',
            'ps': '',
            'add': node.get('add', ''),
            'port': node.get('port', 0),
            'id': node.get('id', ''),
            'aid': node.get('aid', 0),
            'net': node.get('net', 'tcp'),
            'type': node.get('type', 'none'),
            'host': node.get('host', ''),
            'path': node.get('path', ''),
            'tls': node.get('tls', ''),
            'sni': node.get('sni', ''),
        }
        b64 = base64.b64encode(json.dumps(data).encode()).decode()
        return 'vmess://' + b64
    elif node.get('type') == 'ss':
        if node.get('raw') and node['raw'].startswith('ss://'):
            return node['raw']
        auth = f"{node.get('method','')}:{node.get('password','')}"
        auth_b64 = base64.b64encode(auth.encode()).decode()
        return f"ss://{auth_b64}@{node.get('add','')}:{node.get('port',0)}"
    elif node.get('type') == 'trojan':
        if node.get('raw') and node['raw'].startswith('trojan://'):
            return node['raw']
        netloc = f"{node.get('password','')}@{node.get('add','')}:{node.get('port',443)}"
        return f"trojan://{netloc}"
    else:
        return node.get('raw', '')

def nodes_to_clash_yaml(nodes):
    proxies = []
    for idx, node in enumerate(nodes):
        proxy = {
            'name': f"node-{idx+1}",
            'type': node.get('type', 'vmess'),
            'server': node.get('add', ''),
            'port': node.get('port', 0),
        }
        if node.get('type') == 'vmess':
            proxy['uuid'] = node.get('id', '')
            proxy['alterId'] = node.get('aid', 0)
            proxy['cipher'] = node.get('cipher', 'auto')
            proxy['network'] = node.get('net', 'tcp')
            if node.get('tls'):
                proxy['tls'] = True
            if node.get('sni'):
                proxy['sni'] = node.get('sni')
            if node.get('host'):
                proxy['host'] = node.get('host')
            if node.get('path'):
                proxy['path'] = node.get('path')
        elif node.get('type') == 'ss':
            proxy['cipher'] = node.get('method', '')
            proxy['password'] = node.get('password', '')
        elif node.get('type') == 'trojan':
            proxy['password'] = node.get('password', '')
            if node.get('sni'):
                proxy['sni'] = node.get('sni')
            if node.get('allowInsecure') == '1':
                proxy['skip-cert-verify'] = True
        proxies.append(proxy)
    clash_data = {'proxies': proxies}
    return yaml.dump(clash_data, default_flow_style=False, allow_unicode=True)

def nodes_to_base64(nodes):
    links = []
    for node in nodes:
        link = node_to_vmess_link(node)
        if link:
            links.append(link)
    raw = '\n'.join(links)
    return base64.b64encode(raw.encode()).decode()

# ======================== 主爬虫类 ========================

class Crawler:
    def __init__(self):
        self.visited_urls = set()
        self.queue = deque()
        self.all_nodes = []
        self.request_count = 0

    def process_source_line(self, line):
        line = line.strip()
        if not line or line.startswith('#'):
            return None

        if line.startswith('+date'):
            parts = line.split(maxsplit=1)
            if len(parts) < 2:
                return None
            url_template = parts[1].strip()
            try:
                url = datetime.now().strftime(url_template)
            except:
                url = url_template
        else:
            url = line

        if '*' in url:
            base_part = url.split('*', 1)[0]
            if not base_part.startswith(('http://', 'https://')):
                return None
            base_url = base_part.rstrip('/')
            pattern = re.escape(url).replace('\\*', '.*')
            regex = re.compile('^' + pattern + '$', re.IGNORECASE)
            return ('page', base_url, 1, regex)
        else:
            if is_node_link(url):
                return ('direct', url, None, None)
            else:
                return ('page', url, 1, None)

    def enqueue(self, url, depth, pattern):
        if url in self.visited_urls:
            return
        if self.request_count >= MAX_REQUESTS:
            return
        self.queue.append((url, depth, pattern))

    def download_subscription(self, url):
        if url in self.visited_urls:
            return
        self.visited_urls.add(url)
        self.request_count += 1
        print(f"[{self.request_count}/{MAX_REQUESTS}] Downloading subscription: {url}")
        try:
            content = fetch_binary(url)
            if content is None:
                print(f"  -> No content (fetch failed)")
                return
            try:
                text = content.decode('utf-8', errors='ignore')
            except:
                text = ''
            if not text:
                print(f"  -> Empty content")
                return
            nodes = parse_subscription_content(text, url)
            if nodes:
                for node in nodes:
                    if 'raw' not in node or not node['raw']:
                        node['raw'] = node_to_vmess_link(node)
                self.all_nodes.extend(nodes)
                print(f"  -> Found {len(nodes)} nodes")
            else:
                print(f"  -> No nodes parsed")
        except Exception as e:
            print(f"  -> Error: {e}")

    def parse_page(self, html, base_url, depth, pattern):
        soup = BeautifulSoup(html, 'html.parser')

        # 处理 a[href]
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = safe_urljoin(base_url, href)
            if not full_url:
                continue
            full_url = clean_url(full_url)
            if full_url in self.visited_urls:
                continue

            if pattern and pattern.match(full_url):
                if depth < MAX_DEPTH:
                    self.enqueue(full_url, depth + 1, pattern)
                if any(kw.lower() in full_url.lower() for kw in KEYWORDS):
                    self.download_subscription(full_url)
                    if depth < MAX_DEPTH and full_url not in self.visited_urls:
                        self.enqueue(full_url, depth + 1, pattern)
                continue

            if any(kw.lower() in full_url.lower() for kw in KEYWORDS):
                self.download_subscription(full_url)
                if depth < MAX_DEPTH and full_url not in self.visited_urls:
                    self.enqueue(full_url, depth + 1, pattern)

        # 从纯文本提取 URL
        text = soup.get_text()
        url_regex = re.compile(r'https?://[^\s<>"\'{}|\\^`\[\]]+', re.IGNORECASE)
        for match in url_regex.findall(text):
            url = clean_url(match)
            if url in self.visited_urls:
                continue
            if pattern and pattern.match(url):
                if depth < MAX_DEPTH:
                    self.enqueue(url, depth + 1, pattern)
            elif any(kw.lower() in url.lower() for kw in KEYWORDS):
                self.download_subscription(url)
                if depth < MAX_DEPTH and url not in self.visited_urls:
                    self.enqueue(url, depth + 1, pattern)

        # 提取直接节点链接（vmess:// 等）
        node_link_regex = re.compile(r'(vmess|ss|trojan)://[^\s<>"\'{}|\\^`\[\]]+', re.IGNORECASE)
        for match in node_link_regex.findall(text):
            link = match.strip()
            node = parse_node_link(link)
            if node:
                node['raw'] = link
                self.all_nodes.append(node)
                print(f"  Found direct node link: {link[:50]}...")

    def crawl(self, source_lines):
        valid_lines = [l for l in source_lines if l.strip() and not l.strip().startswith('#')]
        if not valid_lines:
            print("⚠️  No valid source lines found in crawler.list.")
            return
        print(f"✅ Loaded {len(valid_lines)} source lines.")

        for line in valid_lines:
            task = self.process_source_line(line)
            if task is None:
                print(f"⚠️  Skipped invalid line: {line}")
                continue
            task_type, target, depth, pattern = task
            if task_type == 'direct':
                self.download_subscription(target)
            elif task_type == 'page':
                self.enqueue(target, depth, pattern)

        print(f"📋 Initial queue size: {len(self.queue)}")

        while self.queue and self.request_count < MAX_REQUESTS:
            url, depth, pattern = self.queue.popleft()
            if url in self.visited_urls:
                continue
            self.visited_urls.add(url)
            self.request_count += 1
            print(f"[{self.request_count}/{MAX_REQUESTS}] Crawling: {url} (depth={depth})")
            try:
                html = fetch_content(url)
                if html is None:
                    continue
                self.parse_page(html, url, depth, pattern)
            except Exception as e:
                print(f"  Error crawling {url}: {e}")

        print(f"🏁 Crawl finished. Total requests: {self.request_count}, Nodes collected: {len(self.all_nodes)}")

    def dedupe_and_test(self):
        print("🔄 Deduplicating...")
        unique = {}
        for node in self.all_nodes:
            key = normalize_node_key(node)
            if key not in unique:
                unique[key] = node
        nodes = list(unique.values())
        print(f"   After dedupe: {len(nodes)} nodes")

        print("🌐 Testing connectivity...")
        valid_nodes = []
        with ThreadPoolExecutor(max_workers=TEST_WORKERS) as executor:
            future_to_node = {executor.submit(test_node_connectivity, node): node for node in nodes}
            for future in as_completed(future_to_node):
                node = future_to_node[future]
                try:
                    if future.result():
                        valid_nodes.append(node)
                    else:
                        print(f"   ❌ Unreachable: {node.get('add')}:{node.get('port')}")
                except Exception as e:
                    print(f"   ⚠️  Test error: {e}")
        print(f"   After connectivity test: {len(valid_nodes)} nodes")
        return valid_nodes

    def run(self):
        if not os.path.exists(SOURCE_FILE):
            print(f"❌ Error: Source file '{SOURCE_FILE}' not found.")
            sys.exit(1)

        with open(SOURCE_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        self.crawl(lines)

        if not self.all_nodes:
            print("❌ No nodes collected. Check your sources and network.")
            return

        valid_nodes = self.dedupe_and_test()

        if not valid_nodes:
            print("❌ No valid nodes after testing. No output files generated.")
            return

        clash_yaml = nodes_to_clash_yaml(valid_nodes)
        with open(OUTPUT_YAML, 'w', encoding='utf-8') as f:
            f.write(clash_yaml)
        print(f"✅ Clash YAML written to {OUTPUT_YAML}")

        base64_str = nodes_to_base64(valid_nodes)
        with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
            f.write(base64_str)
        print(f"✅ Base64 subscription written to {OUTPUT_TXT}")

# ======================== 主入口 ========================
if __name__ == '__main__':
    crawler = Crawler()
    crawler.run()

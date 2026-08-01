#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
crawler.py - 多源节点爬取、合并、去重与连通性测试工具

功能概述：
    1. 从 crawler.list 读取订阅源（支持通配符、动态日期、直接链接、网页爬取）。
    2. 根据关键字（如 'subscri', 'feed', '.yaml' 等）或通配符模式，递归爬取网页，提取节点。
    3. 解析 Clash / V2Ray 订阅内容（支持 YAML、Base64、普通节点链接）。
    4. 将收集到的节点去重、连通性测试，输出 Clash 标准 YAML 和 V2Ray 标准 Base64 文件。

使用方法：
    1. 配置 crawler.list（每行一个源，支持以下格式）：
        - 直接订阅链接：以 .yaml / .yml / .txt 结尾，或内容为节点格式。
        - 网页链接：无通配符，直接抓取该页面。
        - 带通配符的链接：例如 https://www.cfmem.com/*/*/*.html ，从基础 URL 开始爬取，匹配通配符的链接入队。
        - 带 +date 前缀：例如 +date https://nodefree.org/dy/%Y/%m/%Y%m%d.yaml ，自动替换日期占位符。
    2. 运行：python crawler.py
    3. 输出：crawclash.yaml（Clash 配置） 和 crawsub.txt（Base64 编码的节点链接列表）。

注意事项：
    - 本程序使用 requests 和 BeautifulSoup 进行网页抓取，请确保网络环境允许。
    - 爬取深度、总请求数、超时等参数在全局变量中配置，可根据需要调整。
    - 连通性测试会尝试 TCP 连接每个节点的地址:端口，超时 3 秒，失败则丢弃。
    - 对于 Telegram 等动态页面，仅抓取静态 HTML，可能无法获取全部内容，建议直接使用订阅链接。
    - 关键字和通配符匹配不区分大小写。
"""

import requests
import re
import yaml
import base64
import json
import socket
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import time
import hashlib

# ======================== 全局配置参数（可修改） ========================
# 爬取深度（从起始页算起，1 表示只抓起始页，2 表示再抓一层链接，以此类推）
MAX_DEPTH = 2

# 总请求数限制（包括网页抓取和订阅下载）
MAX_REQUESTS = 150

# 单次 HTTP 请求超时（秒）
REQUEST_TIMEOUT = 30

# 关键词列表（用于识别订阅链接）
KEYWORDS = ['subscri', 'feed', '.yaml', '.yml', '.txt']
# 注意：关键词匹配为包含关系，不区分大小写

# 输出文件名
OUTPUT_YAML = 'crawclash.yaml'
OUTPUT_TXT = 'crawsub.txt'

# 订阅源列表文件名（可修改）
SOURCE_FILE = 'crawler.list'

# 连通性测试超时（秒）
CONNECT_TIMEOUT = 3

# 并发测试线程数
TEST_WORKERS = 20

# ======================== 辅助函数 ========================

def clean_url(url):
    """去除 URL 末尾常见的标点符号"""
    return re.sub(r'[:,.?!;]+$', '', url.strip())

def safe_urljoin(base, url):
    """安全的 urljoin，处理相对路径"""
    if not url:
        return None
    try:
        return urljoin(base, url)
    except:
        return None

def is_node_link(url):
    """判断一个 URL 是否可能是节点订阅链接（基于扩展名或常见模式）"""
    if not url:
        return False
    lower = url.lower()
    # 常见节点文件扩展名
    if lower.endswith(('.yaml', '.yml', '.txt')):
        return True
    # 包含订阅关键字
    for kw in KEYWORDS:
        if kw.lower() in lower:
            return True
    return False

def fetch_content(url, retries=3, delay=1):
    """获取 URL 内容，带重试"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    }
    for i in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            # 检测编码
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
    """获取二进制内容（用于订阅文件）"""
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

# ======================== 节点解析核心 ========================

def parse_vmess(vmess_url):
    """解析 vmess:// 链接，返回节点字典（标准化字段）"""
    if not vmess_url.startswith('vmess://'):
        return None
    try:
        b64 = vmess_url[8:]
        # 补齐 padding
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
    """解析 ss:// 链接"""
    if not ss_url.startswith('ss://'):
        return None
    try:
        content = ss_url[5:]
        # 可能包含 @ 或直接 base64
        if '@' in content:
            # 格式 ss://base64(method:password)@host:port
            prefix, suffix = content.split('@', 1)
            # prefix 是 base64 编码的 method:password
            b64 = prefix.replace('-', '+').replace('_', '/')
            b64 += '=' * (4 - len(b64) % 4)
            decoded = base64.b64decode(b64).decode('utf-8')
            method, password = decoded.split(':', 1)
            host, port = suffix.split(':')
            port = int(port)
        else:
            # 格式 ss://base64(method:password@host:port)
            b64 = content.replace('-', '+').replace('_', '/')
            b64 += '=' * (4 - len(b64) % 4)
            decoded = base64.b64decode(b64).decode('utf-8')
            # 格式 method:password@host:port
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
    """解析 trojan:// 链接"""
    if not trojan_url.startswith('trojan://'):
        return None
    try:
        # 格式 trojan://password@host:port?allowInsecure=1#name
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
    """解析单个节点链接（vmess/ss/trojan），返回标准节点字典或 None"""
    if link.startswith('vmess://'):
        return parse_vmess(link)
    elif link.startswith('ss://'):
        return parse_ss(link)
    elif link.startswith('trojan://'):
        return parse_trojan(link)
    else:
        return None

def parse_subscription_content(content, url_hint=''):
    """
    解析订阅内容，返回节点列表（每个节点为标准字典，包含原始链接字段 'raw'）
    """
    nodes = []
    if not content:
        return nodes

    # 尝试作为 Clash YAML 解析
    try:
        data = yaml.safe_load(content)
        if isinstance(data, dict) and 'proxies' in data:
            # Clash 订阅
            for proxy in data['proxies']:
                # 构造标准化节点
                node = {
                    'type': proxy.get('type', ''),
                    'add': proxy.get('server', ''),
                    'port': int(proxy.get('port', 0)),
                    'uuid': proxy.get('uuid', ''),
                    'aid': proxy.get('alterId', 0),
                    'cipher': proxy.get('cipher', ''),
                    'network': proxy.get('network', 'tcp'),
                    'tls': proxy.get('tls', False),
                    'sni': proxy.get('sni', ''),
                    'host': proxy.get('host', ''),
                    'path': proxy.get('path', ''),
                    'raw': f"{proxy.get('type', '')}://{proxy.get('server', '')}:{proxy.get('port', '')}"  # 占位
                }
                nodes.append(node)
            return nodes
    except:
        pass

    # 尝试 Base64 解码（可能是 V2Ray 订阅）
    try:
        # 如果内容是纯 Base64 字符（无空格，长度是4的倍数）
        b64_clean = content.replace('\n', '').replace('\r', '').replace(' ', '')
        if re.match(r'^[A-Za-z0-9+/=]+$', b64_clean) and len(b64_clean) % 4 == 0:
            decoded = base64.b64decode(b64_clean).decode('utf-8', errors='ignore')
            # 按行分割，每行可能是一个节点链接
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

    # 尝试作为普通文本，每行一个节点链接
    for line in content.splitlines():
        line = line.strip()
        if line and (line.startswith(('vmess://', 'ss://', 'trojan://'))):
            node = parse_node_link(line)
            if node:
                node['raw'] = line
                nodes.append(node)

    return nodes

def normalize_node_key(node):
    """
    生成节点的唯一标识，用于去重。
    基于 (类型, 地址, 端口, 协议特定标识)
    """
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
    """测试节点 TCP 连通性，返回是否可达"""
    host = node.get('add', '')
    port = node.get('port', 0)
    if not host or not port:
        return False
    try:
        # 解析域名
        ip = socket.gethostbyname(host)
        with socket.create_connection((ip, port), timeout=CONNECT_TIMEOUT):
            return True
    except:
        return False

def node_to_vmess_link(node):
    """将节点字典转为 vmess:// 链接（若可能）"""
    if node.get('type') == 'vmess':
        # 尝试重建 vmess 链接
        if node.get('raw') and node['raw'].startswith('vmess://'):
            return node['raw']
        # 否则构造
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
        # 构造 ss://
        auth = f"{node.get('method','')}:{node.get('password','')}"
        auth_b64 = base64.b64encode(auth.encode()).decode()
        return f"ss://{auth_b64}@{node.get('add','')}:{node.get('port',0)}"
    elif node.get('type') == 'trojan':
        if node.get('raw') and node['raw'].startswith('trojan://'):
            return node['raw']
        # 构造 trojan://
        netloc = f"{node.get('password','')}@{node.get('add','')}:{node.get('port',443)}"
        return f"trojan://{netloc}"
    else:
        return node.get('raw', '')

def nodes_to_clash_yaml(nodes):
    """将节点列表转换为 Clash YAML 字符串"""
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
    """将节点列表转换为 Base64 编码（每行一个节点链接）"""
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
        self.visited_urls = set()          # 已访问 URL（去重）
        self.queue = deque()               # 待爬取队列 (url, depth, pattern)
        self.all_nodes = []                # 收集的节点列表
        self.request_count = 0
        self.found_links = set()           # 用于去重爬取链接

    def process_source_line(self, line):
        """处理 crawler.list 中的一行，返回起始爬取任务或直接解析"""
        line = line.strip()
        if not line or line.startswith('#'):
            return None

        # 处理 +date 动态日期
        if line.startswith('+date'):
            # 提取 URL 部分
            parts = line.split(maxsplit=1)
            if len(parts) < 2:
                return None
            url_template = parts[1].strip()
            # 用当前时间替换占位符（如 %Y/%m/%Y%m%d）
            now = datetime.now()
            try:
                url = now.strftime(url_template)
            except:
                url = url_template  # 无法解析则原样
        else:
            url = line

        # 判断是否包含通配符 *
        if '*' in url:
            # 提取基础 URL（第一个 * 之前的部分）
            base_part = url.split('*', 1)[0]
            # 如果 base_part 不包含协议，则可能是相对路径，但通常有完整域名，我们取到 * 前的部分
            # 确保 base_part 是有效的 URL 开头
            if not base_part.startswith(('http://', 'https://')):
                # 如果 base_part 为空或只有 /，则基础 URL 可能是域名？但无法确定，跳过
                return None
            # 基础 URL 可能包含路径，我们保留到 * 前
            base_url = base_part.rstrip('/')
            # 将通配符模式转为正则
            pattern = re.escape(url).replace('\\*', '.*')
            regex = re.compile('^' + pattern + '$', re.IGNORECASE)
            # 起始任务：爬取 base_url
            return ('page', base_url, 1, regex)
        else:
            # 无通配符，可能是直接订阅或普通网页
            # 如果是直接订阅（扩展名匹配或内容类型），直接下载解析
            if is_node_link(url):
                return ('direct', url, None, None)
            else:
                return ('page', url, 1, None)

    def crawl(self, source_lines):
        """主爬取流程"""
        # 解析所有源，加入队列
        for line in source_lines:
            task = self.process_source_line(line)
            if task is None:
                continue
            task_type, target, depth, pattern = task
            if task_type == 'direct':
                # 直接下载订阅
                self.download_subscription(target)
            elif task_type == 'page':
                self.enqueue(target, depth, pattern)

        # 执行爬取队列
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
                print(f"Error crawling {url}: {e}")

        print(f"Total requests: {self.request_count}, Nodes collected: {len(self.all_nodes)}")

    def enqueue(self, url, depth, pattern):
        """将 URL 加入爬取队列（去重）"""
        if url in self.visited_urls:
            return
        if self.request_count >= MAX_REQUESTS:
            return
        self.queue.append((url, depth, pattern))

    def download_subscription(self, url):
        """下载并解析订阅链接，将节点加入 all_nodes"""
        if url in self.visited_urls:
            return
        self.visited_urls.add(url)
        self.request_count += 1
        print(f"[{self.request_count}/{MAX_REQUESTS}] Downloading subscription: {url}")
        try:
            content = fetch_binary(url)
            if content is None:
                return
            # 尝试解码为文本
            try:
                text = content.decode('utf-8', errors='ignore')
            except:
                text = ''
            if not text:
                return
            nodes = parse_subscription_content(text, url)
            if nodes:
                # 提取原始链接
                for node in nodes:
                    # 确保有 raw 字段，如果没有，尝试根据类型生成
                    if 'raw' not in node or not node['raw']:
                        # 尝试生成链接
                        node['raw'] = node_to_vmess_link(node)
                self.all_nodes.extend(nodes)
                print(f"  Found {len(nodes)} nodes from {url}")
            else:
                # 可能不是节点，但可能包含链接，尝试作为普通网页继续爬取？
                # 不直接爬取，因为没有深度信息，忽略
                pass
        except Exception as e:
            print(f"Error downloading {url}: {e}")

    def parse_page(self, html, base_url, depth, pattern):
        """
        解析页面内容，提取所有链接：
        1. 如果链接匹配 pattern（通配符模式），入队继续爬取。
        2. 如果链接包含关键字（如 'subscri'），尝试下载解析订阅；若解析失败，也入队继续爬取。
        同时，从页面文本中提取纯文本 URL 进行同样处理。
        """
        soup = BeautifulSoup(html, 'html.parser')

        # 收集所有链接（a[href]）
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = safe_urljoin(base_url, href)
            if not full_url:
                continue
            full_url = clean_url(full_url)
            if full_url in self.visited_urls:
                continue

            # 检查通配符模式
            if pattern and pattern.match(full_url):
                # 符合通配符，入队爬取（深度+1）
                if depth < MAX_DEPTH:
                    self.enqueue(full_url, depth + 1, pattern)
                continue  # 匹配通配符的链接不再当作关键字处理？但需求说同时符合关键字也要处理，但避免重复，可以都处理

            # 检查是否包含关键字
            if any(kw.lower() in full_url.lower() for kw in KEYWORDS):
                # 尝试下载解析
                self.download_subscription(full_url)
                # 注意：下载后即使不是节点，也不再爬取，因为深度未知；但需求说如果非节点则继续爬取，所以我们也可以入队
                # 这里我们选择：如果下载后没有解析到节点，则当作网页入队继续爬取（深度+1）
                # 但为了防止重复，我们检查是否已经访问过
                if full_url not in self.visited_urls:
                    # 尝试下载并判断是否为节点
                    # 但 download_subscription 已经尝试下载并可能解析，如果没解析到节点，我们可以入队
                    # 不过我们没有标志，所以我们简单把这类链接入队（深度+1）
                    if depth < MAX_DEPTH:
                        self.enqueue(full_url, depth + 1, pattern)

        # 从页面文本中提取 URL（使用正则）
        text = soup.get_text()
        url_regex = re.compile(r'https?://[^\s<>"\'{}|\\^`\[\]]+', re.IGNORECASE)
        for match in url_regex.findall(text):
            url = clean_url(match)
            if url in self.visited_urls:
                continue
            # 同样处理：匹配通配符或关键字
            if pattern and pattern.match(url):
                if depth < MAX_DEPTH:
                    self.enqueue(url, depth + 1, pattern)
            elif any(kw.lower() in url.lower() for kw in KEYWORDS):
                self.download_subscription(url)
                if depth < MAX_DEPTH and url not in self.visited_urls:
                    self.enqueue(url, depth + 1, pattern)

    def dedupe_and_test(self):
        """去重并测试连通性，返回有效节点列表"""
        print("Deduplicating and testing connectivity...")
        unique = {}
        for node in self.all_nodes:
            key = normalize_node_key(node)
            if key not in unique:
                unique[key] = node

        nodes = list(unique.values())
        print(f"After dedupe: {len(nodes)} nodes")

        # 连通性测试
        valid_nodes = []
        with ThreadPoolExecutor(max_workers=TEST_WORKERS) as executor:
            future_to_node = {executor.submit(test_node_connectivity, node): node for node in nodes}
            for future in as_completed(future_to_node):
                node = future_to_node[future]
                try:
                    if future.result():
                        valid_nodes.append(node)
                    else:
                        print(f"Node {node.get('add')}:{node.get('port')} unreachable")
                except:
                    pass

        print(f"After connectivity test: {len(valid_nodes)} nodes")
        return valid_nodes

    def run(self):
        """运行整个流程，使用全局 SOURCE_FILE 读取源列表"""
        with open(SOURCE_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        self.crawl(lines)

        if not self.all_nodes:
            print("No nodes found.")
            return

        # 去重与连通性测试
        valid_nodes = self.dedupe_and_test()

        if not valid_nodes:
            print("No valid nodes after testing.")
            return

        # 生成输出
        clash_yaml = nodes_to_clash_yaml(valid_nodes)
        with open(OUTPUT_YAML, 'w', encoding='utf-8') as f:
            f.write(clash_yaml)
        print(f"Clash YAML written to {OUTPUT_YAML}")

        base64_str = nodes_to_base64(valid_nodes)
        with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
            f.write(base64_str)
        print(f"Base64 subscription written to {OUTPUT_TXT}")

# ======================== 主程序入口 ========================

if __name__ == '__main__':
    crawler = Crawler()
    crawler.run()

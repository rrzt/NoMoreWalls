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
MAX_DEPTH = 3                # 增大深度，便于匹配多层通配符
MAX_REQUESTS = 300           # 增加请求数
REQUEST_TIMEOUT = 30
KEYWORDS = ['subscri', 'feed', '.yaml', '.yml', '.txt']
OUTPUT_YAML = 'crawclash.yaml'
OUTPUT_TXT = 'crawsub.txt'
SOURCE_FILE = 'crawler.list'
CONNECT_TIMEOUT = 3
TEST_WORKERS = 20

# ======================== 辅助函数（同上，省略） ========================
# 此处保留原有的 clean_url, safe_urljoin, is_node_link, fetch_content, fetch_binary
# 以及节点解析函数 parse_vmess, parse_ss, parse_trojan, parse_node_link,
# parse_subscription_content, normalize_node_key, test_node_connectivity,
# node_to_vmess_link, nodes_to_clash_yaml, nodes_to_base64

# 为节省篇幅，我假设这些函数与原代码完全相同，仅改动主爬虫类
# ============================================================

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

        # 处理 +date
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

        # 收集所有 a[href]
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = safe_urljoin(base_url, href)
            if not full_url:
                continue
            full_url = clean_url(full_url)
            if full_url in self.visited_urls:
                continue

            # 通配符匹配 -> 入队继续爬取
            if pattern and pattern.match(full_url):
                if depth < MAX_DEPTH:
                    self.enqueue(full_url, depth + 1, pattern)
                # 同时检查是否也是订阅链接，如果是，也尝试下载（但不能用 continue，要同时处理）
                if any(kw.lower() in full_url.lower() for kw in KEYWORDS):
                    self.download_subscription(full_url)
                    if depth < MAX_DEPTH and full_url not in self.visited_urls:
                        self.enqueue(full_url, depth + 1, pattern)
                continue

            # 检查关键字
            if any(kw.lower() in full_url.lower() for kw in KEYWORDS):
                self.download_subscription(full_url)
                # 如果下载后没有节点，仍可继续爬取（但需防止重复）
                if depth < MAX_DEPTH and full_url not in self.visited_urls:
                    self.enqueue(full_url, depth + 1, pattern)

        # 从页面纯文本中提取 URL（包括 pre/code 等标签）
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

        # 额外：从页面中提取 vmess://、ss:// 等直链（可能出现在文本中）
        node_link_regex = re.compile(r'(vmess|ss|trojan)://[^\s<>"\'{}|\\^`\[\]]+', re.IGNORECASE)
        for match in node_link_regex.findall(text):
            link = match.strip()
            node = parse_node_link(link)
            if node:
                node['raw'] = link
                self.all_nodes.append(node)
                print(f"  Found direct node link: {link[:50]}...")

    def crawl(self, source_lines):
        # 过滤有效行
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
        # 检查源文件是否存在
        if not os.path.exists(SOURCE_FILE):
            print(f"❌ Error: Source file '{SOURCE_FILE}' not found in current directory.")
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

        # 生成输出
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

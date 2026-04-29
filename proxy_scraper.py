#!/usr/bin/env python3
"""
High-Performance Proxy Scraper and Checker

Scrapes free proxies from multiple public sources, verifies them concurrently,
measures latency, and saves working proxies to a file.
"""

import asyncio
import aiohttp
import re
from typing import List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import sys


@dataclass
class Proxy:
    ip: str
    port: str
    source: str
    
    def __str__(self) -> str:
        return f"{self.ip}:{self.port}"


class ProxyScraper:
    """Scrapes proxies from multiple public sources."""
    
    SOURCES = [
        {
            "name": "free-proxy-list.net",
            "url": "https://free-proxy-list.net/",
            "pattern": r'<td>(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})</td>\s*<td>(\d+)</td>'
        },
        {
            "name": "sslproxies.org",
            "url": "https://www.sslproxies.org/",
            "pattern": r'<td>(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})</td>\s*<td>(\d+)</td>'
        },
        {
            "name": "us-proxy.org",
            "url": "https://www.us-proxy.org/",
            "pattern": r'<td>(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})</td>\s*<td>(\d+)</td>'
        }
    ]
    
    def __init__(self):
        self.proxies: List[Proxy] = []
    
    async def scrape_source(self, session: aiohttp.ClientSession, source: dict) -> List[Proxy]:
        """Scrape proxies from a single source."""
        found_proxies = []
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            async with session.get(source["url"], headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 200:
                    html = await response.text()
                    matches = re.findall(source["pattern"], html)
                    for ip, port in matches:
                        proxy = Proxy(ip=ip, port=port, source=source["name"])
                        found_proxies.append(proxy)
                    print(f"[✓] Scraped {len(found_proxies)} proxies from {source['name']}")
                else:
                    print(f"[✗] Failed to fetch {source['name']} (Status: {response.status})")
        except asyncio.TimeoutError:
            print(f"[✗] Timeout scraping {source['name']}")
        except Exception as e:
            print(f"[✗] Error scraping {source['name']}: {str(e)}")
        
        return found_proxies
    
    async def scrape_all(self) -> List[Proxy]:
        """Scrape proxies from all sources concurrently."""
        print("\n" + "="*60)
        print("🕷️  STARTING PROXY SCRAPING")
        print("="*60 + "\n")
        
        async with aiohttp.ClientSession() as session:
            tasks = [self.scrape_source(session, source) for source in self.SOURCES]
            results = await asyncio.gather(*tasks)
            
            for proxy_list in results:
                self.proxies.extend(proxy_list)
        
        # Remove duplicates
        unique_proxies = []
        seen = set()
        for proxy in self.proxies:
            proxy_str = str(proxy)
            if proxy_str not in seen:
                seen.add(proxy_str)
                unique_proxies.append(proxy)
        
        self.proxies = unique_proxies
        print(f"\n📊 Total unique proxies collected: {len(self.proxies)}")
        return self.proxies


class ProxyChecker:
    """Verifies proxies concurrently and measures latency."""
    
    TEST_URL = "http://httpbin.org/ip"
    # Alternative test URLs for redundancy
    FALLBACK_URLS = [
        "https://api.ipify.org?format=json",
        "http://icanhazip.com"
    ]
    
    def __init__(self, timeout: int = 8, max_concurrent: int = 50):
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self.working_proxies: List[Tuple[Proxy, float]] = []
        self.checked_count = 0
        self.failed_count = 0
    
    async def check_proxy(self, session: aiohttp.ClientSession, proxy: Proxy, semaphore: asyncio.Semaphore) -> Optional[Tuple[Proxy, float]]:
        """Check a single proxy's validity and measure response time."""
        async with semaphore:
            proxy_url = f"http://{proxy.ip}:{proxy.port}"
            
            try:
                start_time = asyncio.get_event_loop().time()
                
                # Create a new connector for each proxy check to avoid connection reuse issues
                connector = aiohttp.TCPConnector(ssl=False, force_close=True, enable_cleanup_closed=True)
                local_session = aiohttp.ClientSession(connector=connector)
                
                try:
                    async with local_session.get(
                        self.TEST_URL,
                        proxy=proxy_url,
                        timeout=aiohttp.ClientTimeout(total=self.timeout)
                    ) as response:
                        if response.status == 200:
                            elapsed = asyncio.get_event_loop().time() - start_time
                            elapsed_ms = round(elapsed * 1000, 2)
                            
                            # Verify the response actually came through the proxy
                            try:
                                data = await response.json()
                                if data:
                                    self.checked_count += 1
                                    self._update_progress(proxy, elapsed_ms, True)
                                    return (proxy, elapsed_ms)
                            except:
                                self.checked_count += 1
                                self._update_progress(proxy, elapsed_ms, True)
                                return (proxy, elapsed_ms)
                        
                        self.failed_count += 1
                        self._update_progress(proxy, None, False)
                        return None
                finally:
                    await local_session.close()
                    await connector.close()
                    
            except asyncio.TimeoutError:
                self.failed_count += 1
                self._update_progress(proxy, None, False)
                return None
            except aiohttp.ClientConnectorError:
                self.failed_count += 1
                self._update_progress(proxy, None, False)
                return None
            except aiohttp.ClientProxyConnectionError:
                self.failed_count += 1
                self._update_progress(proxy, None, False)
                return None
            except aiohttp.ServerDisconnectedError:
                self.failed_count += 1
                self._update_progress(proxy, None, False)
                return None
            except Exception as e:
                self.failed_count += 1
                self._update_progress(proxy, None, False)
                return None
    
    def _update_progress(self, proxy: Proxy, latency: Optional[float], success: bool):
        """Update and display progress in terminal."""
        total = self.checked_count + self.failed_count
        status = "✓" if success else "✗"
        latency_str = f"{latency:.0f}ms" if latency else "timeout"
        
        # Clear line and update progress
        sys.stdout.write(f"\r🔍 Checking: {total} | Working: {self.checked_count} | Failed: {self.failed_count} | Last: {status} {proxy} ({latency_str})")
        sys.stdout.flush()
    
    async def check_all(self, proxies: List[Proxy]) -> List[Tuple[Proxy, float]]:
        """Check all proxies concurrently."""
        print("\n" + "="*60)
        print("⚡ STARTING PROXY VERIFICATION")
        print("="*60 + "\n")
        
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        tasks = [self.check_proxy(None, proxy, semaphore) for proxy in proxies]
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        self.working_proxies = [r for r in results if r is not None]
        
        print("\n")
        return self.working_proxies


class ProxyManager:
    """Main orchestrator for scraping and checking proxies."""
    
    OUTPUT_FILE = "working_proxies.txt"
    
    def __init__(self, timeout: int = 8, max_concurrent: int = 50):
        self.scraper = ProxyScraper()
        self.checker = ProxyChecker(timeout=timeout, max_concurrent=max_concurrent)
    
    def save_proxies(self, proxies_with_latency: List[Tuple[Proxy, float]]):
        """Save working proxies to file with latency information."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(self.OUTPUT_FILE, 'w') as f:
            f.write(f"# Working Proxies - Generated: {timestamp}\n")
            f.write(f"# Total: {len(proxies_with_latency)} proxies\n")
            f.write("# Format: IP:PORT (Latency: Xms)\n\n")
            
            # Sort by latency (fastest first)
            sorted_proxies = sorted(proxies_with_latency, key=lambda x: x[1])
            
            for proxy, latency in sorted_proxies:
                f.write(f"{proxy}  # {latency:.0f}ms - Source: {proxy.source}\n")
        
        print(f"💾 Saved {len(proxies_with_latency)} working proxies to '{self.OUTPUT_FILE}'")
    
    def display_summary(self, proxies_with_latency: List[Tuple[Proxy, float]], total_scraped: int):
        """Display final summary statistics."""
        print("\n" + "="*60)
        print("📈 FINAL SUMMARY")
        print("="*60)
        print(f"Total proxies scraped:     {total_scraped}")
        print(f"Working proxies found:     {len(proxies_with_latency)}")
        print(f"Success rate:              {(len(proxies_with_latency)/total_scraped*100):.1f}%")
        
        if proxies_with_latency:
            latencies = [lat for _, lat in proxies_with_latency]
            avg_latency = sum(latencies) / len(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            
            print(f"\n⏱️  Latency Statistics:")
            print(f"   Fastest:  {min_latency:.0f}ms")
            print(f"   Average:  {avg_latency:.0f}ms")
            print(f"   Slowest:  {max_latency:.0f}ms")
            
            # Top 5 fastest proxies
            print(f"\n🏆 Top 5 Fastest Proxies:")
            sorted_proxies = sorted(proxies_with_latency, key=lambda x: x[1])[:5]
            for i, (proxy, latency) in enumerate(sorted_proxies, 1):
                print(f"   {i}. {proxy} ({latency:.0f}ms) - {proxy.source}")
        
        print("="*60 + "\n")
    
    async def run(self):
        """Execute the full scraping and checking pipeline."""
        start_time = asyncio.get_event_loop().time()
        
        print("\n" + "█"*60)
        print("█  🌐 PROXY SCRAPER & CHECKER                     █")
        print("█  High-Performance Multi-Source Proxy Tool       █")
        print("█"*60)
        
        # Scrape phase
        scraped_proxies = await self.scraper.scrape_all()
        
        if not scraped_proxies:
            print("\n❌ No proxies were scraped. Exiting.")
            return
        
        # Check phase
        working_proxies = await self.checker.check_all(scraped_proxies)
        
        # Save and summarize
        if working_proxies:
            self.save_proxies(working_proxies)
        else:
            print("\n⚠️  No working proxies found!")
        
        self.display_summary(working_proxies, len(scraped_proxies))
        
        elapsed = asyncio.get_event_loop().time() - start_time
        print(f"⏱️  Total execution time: {elapsed:.2f} seconds\n")


async def main():
    """Entry point."""
    # Configuration
    TIMEOUT_SECONDS = 8  # Timeout per proxy request
    MAX_CONCURRENT = 50  # Maximum concurrent checks
    
    manager = ProxyManager(timeout=TIMEOUT_SECONDS, max_concurrent=MAX_CONCURRENT)
    await manager.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        sys.exit(1)

import asyncio
import socket
import os
import hashlib
import json
from pathlib import Path
from typing import List, Optional, Tuple
import aiofiles
from tqdm.asyncio import tqdm

class FileTransferClient:
    def __init__(self, host: str, port: int = 8888):
        self.host = host
        self.port = port
        self.buffer_size = 4 * 1024 * 1024  # 4MB
        
    def scan_directory(self, directory_path: Path, base_path: Path = None) -> List[Tuple[Path, str]]:
        """递归扫描目录，返回文件路径和相对路径的列表"""
        if base_path is None:
            base_path = directory_path
            
        files_list = []
        
        # 使用os.walk递归遍历目录
        for root, dirs, files in os.walk(directory_path):
            root_path = Path(root)
            
            for file in files:
                file_path = root_path / file
                # 计算相对路径
                relative_path = file_path.relative_to(base_path)
                files_list.append((file_path, str(relative_path)))
                
        return files_list

    def get_directory_structure(self, directory_path: Path, base_path: Path = None) -> List[str]:
        """获取目录结构，返回所有需要创建的相对路径"""
        if base_path is None:
            base_path = directory_path
            
        dirs_list = []
        
        for root, dirs, files in os.walk(directory_path):
            root_path = Path(root)
            
            # 添加当前目录的相对路径（除了根目录）
            if root_path != base_path:
                relative_dir = root_path.relative_to(base_path)
                dirs_list.append(str(relative_dir))
                
        return sorted(dirs_list)  # 排序确保父目录先创建

    async def calculate_file_hash(self, filepath: Path) -> str:
        """异步计算文件MD5哈希"""
        hash_md5 = hashlib.md5()
        
        async with aiofiles.open(filepath, 'rb') as f:
            while chunk := await f.read(self.buffer_size):
                hash_md5.update(chunk)
                
        return hash_md5.hexdigest()

    async def create_directory(self, relative_path: str, reader, writer):
        """在服务器上创建目录"""
        command = {
            'type': 'CREATE_DIR',
            'path': relative_path
        }
        
        writer.write((json.dumps(command) + '\n').encode())
        await writer.drain()
        
        # 接收响应
        response_data = await reader.readuntil(b'\n')
        response = json.loads(response_data.decode().strip())
        
        return response['status'] == 'OK'

    async def send_file(self, file_path: Path, relative_path: str, semaphore: asyncio.Semaphore):
        """发送单个文件（支持断点续传）"""
        async with semaphore:  # 限制并发数
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    # 建立连接
                    reader, writer = await asyncio.open_connection(
                        self.host, self.port, family=socket.AF_INET6
                    )
                    
                    file_size = file_path.stat().st_size
                    file_hash = await self.calculate_file_hash(file_path)
                    
                    print(f"[*] 开始传输文件: {relative_path} ({file_size:,} bytes)")
                    
                    # 1. 检查文件状态
                    check_command = {
                        'type': 'CHECK_FILE',
                        'relative_path': relative_path,
                        'size': file_size,
                        'hash': file_hash
                    }
                    
                    writer.write((json.dumps(check_command) + '\n').encode())
                    await writer.drain()
                    
                    # 接收服务器响应
                    response_data = await reader.readuntil(b'\n')
                    response = json.loads(response_data.decode().strip())
                    
                    if response['status'] != 'OK':
                        print(f"[!] 服务器响应错误: {response}")
                        return False
                    
                    resume_pos = response['resume_position']
                    
                    if resume_pos == file_size:
                        print(f"[✓] 文件 {relative_path} 已存在且完整，跳过传输")
                        writer.close()
                        await writer.wait_closed()
                        return True
                    
                    # 2. 开始上传文件
                    upload_command = {
                        'type': 'UPLOAD_FILE',
                        'relative_path': relative_path,
                        'size': file_size,
                        'start_position': resume_pos,
                        'hash': file_hash
                    }
                    
                    writer.write((json.dumps(upload_command) + '\n').encode())
                    await writer.drain()
                    
                    # 创建进度条
                    progress = tqdm(
                        total=file_size,
                        initial=resume_pos,
                        desc=f"发送 {relative_path}",
                        unit='B',
                        unit_scale=True,
                        unit_divisor=1024
                    )
                    
                    # 发送文件数据
                    async with aiofiles.open(file_path, 'rb') as f:
                        await f.seek(resume_pos)
                        bytes_sent = resume_pos
                        
                        while bytes_sent < file_size:
                            remaining = file_size - bytes_sent
                            chunk_size = min(self.buffer_size, remaining)
                            
                            chunk = await f.read(chunk_size)
                            if not chunk:
                                break
                            
                            writer.write(chunk)
                            await writer.drain()
                            
                            bytes_sent += len(chunk)
                            progress.update(len(chunk))
                    
                    progress.close()
                    
                    # 接收完成确认
                    response_data = await reader.readuntil(b'\n')
                    response = json.loads(response_data.decode().strip())
                    
                    writer.close()
                    await writer.wait_closed()
                    
                    if response['status'] == 'COMPLETE':
                        print(f"[✓] 文件 {relative_path} 传输成功")
                        return True
                    else:
                        print(f"[!] 文件 {relative_path} 传输失败: {response}")
                        retry_count += 1
                        
                except Exception as e:
                    print(f"[!] 传输文件 {relative_path} 时出错 (尝试 {retry_count + 1}/{max_retries}): {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        await asyncio.sleep(2 ** retry_count)  # 指数退避
            
            print(f"[!] 文件 {relative_path} 传输失败，已达最大重试次数")
            return False

    async def transfer_directory(self, directory_path: str, max_concurrent: int = 3):
        """传输整个目录"""
        dir_path = Path(directory_path)
        
        if not dir_path.exists():
            print(f"[!] 目录不存在: {directory_path}")
            return
            
        if not dir_path.is_dir():
            print(f"[!] 路径不是目录: {directory_path}")
            return
        
        print(f"[*] 扫描目录: {directory_path}")
        
        # 1. 获取目录结构
        directories = self.get_directory_structure(dir_path)
        print(f"[*] 发现 {len(directories)} 个子目录")
        
        # 2. 获取所有文件
        files = self.scan_directory(dir_path)
        print(f"[*] 发现 {len(files)} 个文件")
        
        if not files and not directories:
            print("[!] 目录为空")
            return
        
        # 3. 先创建目录结构
        if directories:
            print("[*] 创建目录结构...")
            reader, writer = await asyncio.open_connection(
                self.host, self.port, family=socket.AF_INET6
            )
            
            for dir_path_str in directories:
                success = await self.create_directory(dir_path_str, reader, writer)
                if success:
                    print(f"[✓] 创建目录: {dir_path_str}")
                else:
                    print(f"[!] 创建目录失败: {dir_path_str}")
            
            # 发送结束命令
            finish_command = {'type': 'FINISH'}
            writer.write((json.dumps(finish_command) + '\n').encode())
            await writer.drain()
            
            writer.close()
            await writer.wait_closed()
        
        # 4. 并发传输文件
        if files:
            print(f"[*] 开始传输文件，最大并发数: {max_concurrent}")
            
            # 创建信号量限制并发数
            semaphore = asyncio.Semaphore(max_concurrent)
            
            # 创建任务列表
            tasks = [
                self.send_file(file_path, relative_path, semaphore) 
                for file_path, relative_path in files
            ]
            
            # 并发执行所有传输任务
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 统计结果
            success_count = sum(1 for result in results if result is True)
            print(f"\n[*] 目录传输完成: {success_count}/{len(files)} 文件成功")

    async def send_files(self, file_paths: List[str], max_concurrent: int = 3):
        """发送文件列表（兼容原有功能）"""
        files = []
        
        for path_str in file_paths:
            path = Path(path_str)
            if path.is_file():
                files.append((path, path.name))
            elif path.is_dir():
                print(f"[*] 检测到目录: {path_str}，将递归传输")
                await self.transfer_directory(path_str, max_concurrent)
                continue
            else:
                print(f"[!] 路径不存在: {path_str}")
                continue
        
        if files:
            print(f"[*] 准备传输 {len(files)} 个单独文件，最大并发数: {max_concurrent}")
            
            # 创建信号量限制并发数
            semaphore = asyncio.Semaphore(max_concurrent)
            
            # 创建任务列表
            tasks = [
                self.send_file(file_path, relative_path, semaphore) 
                for file_path, relative_path in files
            ]
            
            # 并发执行所有传输任务
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 统计结果
            success_count = sum(1 for result in results if result is True)
            print(f"\n[*] 文件传输完成: {success_count}/{len(files)} 文件成功")

async def main():
    print("=== 增强版文件传输客户端 ===")
    print("支持传输单个文件、多个文件或整个文件夹")
    
    # 获取服务器IPv6地址
    server_ipv6 = input("请输入服务器IPv6地址: ").strip()
    if not server_ipv6:
        print("[!] IPv6地址不能为空")
        return
    
    # 获取传输模式
    print("\n选择传输模式:")
    print("1. 传输单个文件夹")
    print("2. 传输多个文件/文件夹")
    
    mode = input("请选择模式 (1/2): ").strip()
    
    client = FileTransferClient(server_ipv6)
    
    # 获取并发数
    try:
        max_concurrent = int(input("请输入最大并发传输数 (默认3): ") or "3")
    except ValueError:
        max_concurrent = 3
    
    if mode == "1":
        # 单个文件夹模式
        folder_path = input("请输入文件夹路径: ").strip()
        if folder_path:
            await client.transfer_directory(folder_path, max_concurrent)
        else:
            print("[!] 文件夹路径不能为空")
            
    elif mode == "2":
        # 多文件模式
        print("\n请输入要传输的文件/文件夹路径（每行一个，空行结束）：")
        paths = []
        while True:
            path = input().strip()
            if not path:
                break
            paths.append(path)
        
        if paths:
            await client.send_files(paths, max_concurrent)
        else:
            print("[!] 没有输入路径")
    else:
        print("[!] 无效的模式选择")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[*] 传输已取消")

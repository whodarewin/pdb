# PDB
## 测试CASE
1. 读写顺序测试（无ttl写，有ttl写，删除，验证读）
    1. case1测试 无ttl写 finish
        1. 写入1000000行
        2. 读取验证
    2. case2测试 有ttl写 finish  
        1. 写入1000000行 ttl 20s
        2. Thread.sleep(20000)
        3. 读取不到任何数据
    3. case3 删除验证 finish
        1. 写入1000000行
        2. 删除前1000行
        3. 读取验证
2. 读写并发测试
    1. case1
        1. 一个线程一直在写入删除写入删除，并修改状态位置。
        2. 另一个线程根据状态位置判断读取出数据是否正确。
    2. case2
        1. 一个线程一直写入数据，写入的数据放入队列
        2. 另一个线程从队列拿到数据并验证数据
3. 稳定性测试
    1. 断电稳定性
        1. wal稳定性
            1. 写入1000000个数据
            2. 关闭PDB
            3. 在这个路径上重启PDB
            4. 验证写入的数据。
        2. compactor稳定性
            1. 设置compact阀值为1
            2. 写入1000000个数据
            2. 关闭PDB
            3. 在这个路径上重启PDB
            4. 验证写入的数据。
        3. clean 稳定性
4. 组件测试
    1. StateManager 测试
        1. CurrentWal 测试
            1. 创建新的StateManager。
            2. 设置CurrentWal
            3. 关闭StateManager
            4. 重启StateManager
            5. 验证CurrentWal读取出来的和已经有的是否一致。
        2. FlushingWalMeta Test
            1. 创建新的StateManager。
            2. add Flushing Wal META
            3. 关闭StateManager
            4. 重启StateManager
            5. 验证Flusing Wal META读取出来的和已经有的是否一致。
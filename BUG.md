1. get 1出100 fixed,MemCache 获取Iterator出错 fixed.
2. compact 两个文件合并为空，直接删除，不compact。 fixed.
3. 所有的牵扯IO资源的有的类没有释放。
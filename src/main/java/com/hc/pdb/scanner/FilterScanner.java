package com.hc.pdb.scanner;

import com.hc.pdb.Cell;
import com.hc.pdb.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * 最上层的scanner
 * @author han.congcong
 */
@NotThreadSafe
public class FilterScanner implements IScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterScanner.class);
    private PriorityQueue<IScanner> queue;
    private Cell current = null;

    public FilterScanner(Set<IScanner> scanners) {
        if(scanners == null){
            LOGGER.error("scanners can not be null");
            throw new ScannerNullException("scanners can not be null");
        }
        if(scanners.size() == 0){
            LOGGER.info("scanners is empty");
        }
        queue = new PriorityQueue<>((o1, o2) -> Bytes.compare(o1.peek().getKey(),o2.peek().getKey()));
        scanners.forEach((o) -> {
            try {
                o.next();
                queue.add(o);
            }catch (IOException e){
                throw new RuntimeException(e);
            }
        });
    }


    @Override
    public Cell next() throws IOException {
        while(true) {
            IScanner scanner = queue.poll();
            if(scanner == null){
                return null;
            }
            Cell cell = scanner.peek();

            //先过滤掉过期的，丢掉这种Cell
            if((cell.getTtl() != Cell.NO_TTL) && System.currentTimeMillis() > cell.getTimeStamp() + cell.getTtl()) {
                if (scanner.next() != null) {
                    queue.add(scanner);
                }
                continue;
            //处理delete，设置current但是不返回。
            }else if(cell.getDelete()){
                if(scanner.next() != null){
                    queue.add(scanner);
                }
                this.current = cell;
                continue;
            //如果是空，那么直接返回
            } else if(current == null){
                if(scanner.next() != null){
                    queue.add(scanner);
                }
                this.current = cell;
                break;
            //如果和上一个key一致，则为覆盖删除的，继续
            }else if((current != null && (Bytes.compare(cell.getKey(),current.getKey()) == 0))){
                if(scanner.next() != null){
                    queue.add(scanner);
                }
                continue;
            }else{
                if(scanner.next() != null){
                    queue.add(scanner);
                }
                this.current = cell;
                break;
            }
        }
        if(current.getDelete()){
            return null;
        }
        return current;
    }

    @Override
    public Cell peek() {
        return current;
    }
}

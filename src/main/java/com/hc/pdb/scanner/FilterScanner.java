package com.hc.pdb.scanner;

import com.hc.pdb.Cell;
import com.hc.pdb.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * 最上层的scanner
 */
public class FilterScanner implements IScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterScanner.class);
    private PriorityQueue<IScanner> queue;
    private Cell current = null;

    public FilterScanner(Set<IScanner> scanners) {
        if(scanners == null){
            LOGGER.error("scanners can not be null");
            throw new RuntimeException("scanners can not be null");
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
            // 两种情况继续读下一个
            // 1 key和上一个相同
            // 2 为deleteCell
            if((current != null &&
                    (Bytes.compare(cell.getKey(),current.getKey()) == 0)
                    || cell.getDelete())){
                if(scanner.next() != null){
                    queue.add(scanner);
                }
                continue;
            }
            if(scanner.next() != null){
                queue.add(scanner);
            }

            this.current = cell;
            break;
        }

        return current;
    }

    @Override
    public Cell peek() {
        return current;
    }
}
package com.hc.pdb.scanner;

import com.hc.pdb.Cell;
import com.hc.pdb.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 最上层的scanner
 */
public class DefaultScanner implements IScanner {
    private PriorityQueue<IScanner> queue;
    private Cell current = null;

    public DefaultScanner(List<IScanner> scanners) {
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
            Cell cell = scanner.peek();
            if(current != null && Bytes.compare(cell.getKey(),current.getKey()) == 0){
                if(scanner.next() != null){
                    queue.add(scanner);
                }
                //key 相同，选择第一个,todo:抽离此逻辑，并增加delete逻辑
                continue;
            }
            break;
        }

        return current;
    }

    @Override
    public Cell peek() {
        return current;
    }
}

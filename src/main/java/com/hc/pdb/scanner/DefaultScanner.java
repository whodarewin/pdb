package com.hc.pdb.scanner;

import com.hc.pdb.Cell;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 最上层的scanner
 */
public class DefaultScanner implements IScanner {
    private PriorityQueue<IScanner> cells = new PriorityQueue<>();

    public DefaultScanner(List<IScanner> scanners) {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Cell next() throws IOException {
        IScanner scanner = cells.poll();
        Cell ret = scanner.next();
        cells.add(scanner);
        return ret;
    }

    @Override
    public Cell current() throws IOException {
        return cells.peek().current();
    }
}

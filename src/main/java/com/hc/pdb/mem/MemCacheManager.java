package com.hc.pdb.mem;

import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.flusher.Flusher;
import com.hc.pdb.flusher.IFlusher;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.state.StateManager;
import com.hc.pdb.util.Bytes;
import com.hc.pdb.wal.DefaultWalWriter;
import com.hc.pdb.wal.IWalWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * mem cache的manager
 * @author congcong.han
 * @date 2019/6/22
 */
public class MemCacheManager {

    private HCCWriter hccWriter;
    private IFlusher flusher;
    private IWalWriter walWriter;
    private StateManager stateManager;
    private Configuration configuration;
    private List<MemCache> flushingList = Collections.synchronizedList(new ArrayList<>());
    private MemCache current;

    public MemCacheManager(Configuration configuration,StateManager manager) throws IOException {
        hccWriter = new HCCWriter(configuration);
        this.stateManager = manager;
        flusher = new Flusher(configuration, hccWriter, stateManager);
        this.walWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));
        current = new MemCache(configuration);
        this.configuration = configuration;
    }

    public Set<MemCache> searchMemCache(byte[] startKey, byte[] endKey){
        return flushingList.stream().filter(file ->
                Bytes.compare(startKey,file.getEnd()) > 0 && Bytes.compare(endKey,file.getStart()) < 0)
                .collect(Collectors.toSet());
    }

    public void addCell(Cell cell) throws IOException {
        walWriter.write(cell);
        current.put(cell);
        flushIfOK();
    }

    private void flushIfOK() throws IOException {
        if (current.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
            synchronized (this) {
                if (current.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                        PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
                    MemCache tmpCache = current;
                    flushingList.add(tmpCache);
                    IWalWriter tmpWalWriter = walWriter;
                    walWriter.close();
                    walWriter.markFlush();
                    walWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));
                    current = new MemCache(configuration);
                    //todo:flush完毕后，从flushList里面删除
                    flusher.flush(new IFlusher.FlushEntry(tmpCache,tmpWalWriter));
                }
            }
        }
    }
}

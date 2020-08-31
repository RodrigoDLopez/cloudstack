package com.cloud.server;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.GetStorageStatsCommand;
import com.cloud.exception.StorageUnavailableException;
import com.cloud.storage.ImageStoreDetailsUtil;
import com.cloud.storage.StorageManager;
import com.cloud.storage.StorageStats;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreManager;
import org.apache.cloudstack.engine.subsystem.api.storage.EndPoint;
import org.apache.cloudstack.engine.subsystem.api.storage.EndPointSelector;
import org.apache.cloudstack.managed.context.ManagedContextRunnable;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.commons.collections.CollectionUtils;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

class StorageCollector extends ManagedContextRunnable {

    @Inject
    private ImageStoreDetailsUtil imageStoreDetailsUtil;
    @Inject
    private EndPointSelector endPointSelector;
    @Inject
    private StorageManager storageManager;
    @Inject
    private PrimaryDataStoreDao primaryDataStoreDao;
    @Inject
    private DataStoreManager dataStoreMgr;

    private ConcurrentHashMap<Long, StorageStats> secondaryStorageStats = new ConcurrentHashMap<Long, StorageStats>();
    private ConcurrentHashMap<Long, StorageStats> primaryStorageStats = new ConcurrentHashMap<Long, StorageStats>();

    public ConcurrentHashMap<Long, StorageStats> getPrimaryStorageStats() {
        return primaryStorageStats;
    }
    public ConcurrentHashMap<Long, StorageStats> getSecondaryStorageStats() {
        return secondaryStorageStats;
    }

    @Override
    protected void runInContext() {
        try {
            StatsCollector.s_logger.debug("StorageCollector is running.");
            executeCheckForSecondaryStorageStats();
            executeCheckForPrimaryStorageStats();
            StatsCollector.s_logger.debug("StorageCollector is fished.");
        } catch (Throwable t) {
            StatsCollector.s_logger.error("Error trying to retrieve storage stats", t);
        }
    }

    protected void executeCheckForPrimaryStorageStats() {
        ConcurrentHashMap<Long, StorageStats> newStoragePoolStats = new ConcurrentHashMap<Long, StorageStats>();
        List<StoragePoolVO> storagePools = primaryDataStoreDao.listAll();
        if (CollectionUtils.isEmpty(storagePools)) {
            StatsCollector.s_logger.debug(String.format("Could not find primary storages to collect stats"));
        }
        for (StoragePoolVO pool : storagePools) {
            long poolId = pool.getId();
            StatsCollector.s_logger.debug(String.format("Collecting stats for primary storage with storage pool ID = [%d].", poolId));
            // check if the pool has enabled hosts
            List<Long> hostIds = storageManager.getUpHostsInPool(poolId);
            if (CollectionUtils.isEmpty(hostIds)) {
                StatsCollector.s_logger.debug(String.format("Unable find host for the given storage pool ID = [%d]", poolId));
                continue;
            }
            GetStorageStatsCommand command = new GetStorageStatsCommand(pool.getUuid(), pool.getPoolType(), pool.getPath());
            try {
                Answer answer = storageManager.sendToPool(pool, command);
                if (answer == null) {
                    StatsCollector.s_logger.debug(String.format("Unable to receive answer after send the command to storage pool with ID = [%d]", poolId));
                    continue;
                }
                if (answer.getResult()) {
                    newStoragePoolStats.put(pool.getId(), (StorageStats) answer);
                    // Seems like we have dynamically updated the pool size since the prev. size and the current do not match
                    StorageStats storagePool = primaryStorageStats.get(poolId);
                    if (storagePool != null && storagePool.getCapacityBytes() != ((StorageStats) answer).getCapacityBytes()) {
                        StatsCollector.s_logger.debug(String.format("Updating storage pool capacity bytes for the storage pool with ID [%d]", pool.getId()));
                        pool.setCapacityBytes(((StorageStats) answer).getCapacityBytes());
                        primaryDataStoreDao.update(pool.getId(), pool);
                        StatsCollector.s_logger.debug(String.format("Finished. The old capacity was [%d] and the current capacity is [%d]", storagePool.getCapacityBytes(), ((StorageStats) answer).getCapacityBytes()));
                    }
                } else {
                    StatsCollector.s_logger.debug(String.format("Unable to update primary storage pool with ID = [%d] because the answer after send the command to the pool was false", poolId));
                }
            } catch (StorageUnavailableException e) {
                StatsCollector.s_logger.info("Unable to reach " + pool, e);
            } catch (Exception e) {
                StatsCollector.s_logger.warn("Unable to get stats for " + pool, e);
            }
        }
        primaryStorageStats = newStoragePoolStats;
    }

    protected void executeCheckForSecondaryStorageStats() {
        List<DataStore> stores = dataStoreMgr.listImageStores();
        if (CollectionUtils.isEmpty(stores)) {
            StatsCollector.s_logger.debug(String.format("Could not find secondary storages to collect stats"));
        }

        for (DataStore store : stores) {
            long storeId = store.getId();
            StatsCollector.s_logger.debug(String.format("Collecting stats for secondary storage with storage pool ID = [%d].", storeId));
            if (store.getUri() == null) {
                StatsCollector.s_logger.debug(String.format("Unable to find URI for the given secondary storage with ID = [%d]", storeId));
                continue;
            }

            Integer nfsVersion = imageStoreDetailsUtil.getNfsVersion(store.getId());
            GetStorageStatsCommand command = new GetStorageStatsCommand(store.getTO(), nfsVersion);
            EndPoint ssAhost = endPointSelector.select(store);
            if (ssAhost == null) {
                StatsCollector.s_logger.debug("There is no secondary storage VM for secondary storage host " + store.getName());
                continue;
            }
            Answer answer = ssAhost.sendMessage(command);
            if (answer == null) {
                StatsCollector.s_logger.debug(String.format("Unable to receive answer after send the command to Host with ID = [%d]", ssAhost.getId()));
                continue;
            }
            if (answer.getResult()) {
                StatsCollector.s_logger.debug(String.format("Updating data store capacity bytes for the store with ID [%d]", storeId));
                setSecondaryStorageStats(storeId, (StorageStats) answer);
                StatsCollector.s_logger.debug(String.format("Finished. The current capacity is [%d] and the current used is [%d]", ((StorageStats) answer).getCapacityBytes(), ((StorageStats) answer).getByteUsed()));
            } else {
                StatsCollector.s_logger.debug(String.format("Unable to update secondary storage pool with ID = [%d] because the answer after send the command to the pool was false", storeId));
            }
        }
    }

    protected void setSecondaryStorageStats(long storeId, StorageStats answer) {
        secondaryStorageStats.put(storeId, answer);
    }
}

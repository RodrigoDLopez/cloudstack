package com.cloud.server;

import com.cloud.agent.api.GetStorageStatsAnswer;
import com.cloud.agent.api.GetStorageStatsCommand;
import com.cloud.exception.StorageUnavailableException;
import com.cloud.storage.ImageStoreDetailsUtil;
import com.cloud.storage.StorageManager;
import com.cloud.storage.StorageStats;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreManager;
import org.apache.cloudstack.engine.subsystem.api.storage.EndPoint;
import org.apache.cloudstack.engine.subsystem.api.storage.EndPointSelector;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@RunWith(MockitoJUnitRunner.class)
public class StorageCollectorTest {

    @Spy
    @InjectMocks
    private StorageCollector storageCollectorMock;

    @Mock
    private ConcurrentHashMap<Long, StorageStats> storageStatsHashMapMock;

    @Mock
    private DataStore dataStoreMock;

    @Mock
    private DataStoreManager dataStoreMgrMock;

    @Mock
    private EndPoint hostEndPointMock;

    @Mock
    private EndPointSelector endPointSelectorMock;

    @Mock
    private GetStorageStatsAnswer answerMock;

    @Mock
    private ImageStoreDetailsUtil imageStoreDetailsUtilMock;

    @Mock
    private PrimaryDataStoreDao primaryDataStoreDaoMock;

    @Mock
    private StorageManager storageManagerMock;

    @Mock
    private StoragePoolVO storagePoolMock;

    @Mock
    private StorageStats storageStatsMock;
    private Long hostIDMock = 1l;
    private Long storagePoolIDMock = 1l;
    private Long dataStoreIDMock = 1l;
    private String dataStoreURIMock = "AnyString";

    @Test
    public void executeCheckForPrimaryStorageStatsWhenStoragePoolIsEmptyTest() {
        List<StoragePoolVO> storagePoolsMock = new ArrayList<StoragePoolVO>();
        Mockito.doReturn(storagePoolsMock).when(primaryDataStoreDaoMock).listAll();
        storageCollectorMock.executeCheckForPrimaryStorageStats();
        Mockito.verify(storagePoolMock, Mockito.never()).setCapacityBytes(Mockito.anyLong());
    }

    @Test
    public void executeCheckForPrimaryStorageStatsWhenStorageDontHaveHostTest(){
        List<StoragePoolVO> storagePoolsMock = new ArrayList<StoragePoolVO>();
        List<Long> hostIDs = new ArrayList<Long>();
        storagePoolsMock.add(storagePoolMock);

        Mockito.doReturn(storagePoolsMock).when(primaryDataStoreDaoMock).listAll();
        Mockito.doReturn(hostIDs).when(storageManagerMock).getUpHostsInPool(Mockito.anyLong());

        storageCollectorMock.executeCheckForPrimaryStorageStats();
        Mockito.verify(storagePoolMock, Mockito.never()).setCapacityBytes(Mockito.anyLong());
    }

    @Test
    public void executeCheckForPrimaryStorageStatsWithoutAnswerTest() throws StorageUnavailableException {
        List<StoragePoolVO> storagePoolsMock = new ArrayList<StoragePoolVO>();
        List<Long> hostIDs = new ArrayList<Long>();
        GetStorageStatsCommand command = new GetStorageStatsCommand(storagePoolMock.getUuid(), storagePoolMock.getPoolType(), storagePoolMock.getPath());

        storagePoolsMock.add(storagePoolMock);
        hostIDs.add(hostIDMock);

        Mockito.doReturn(storagePoolsMock).when(primaryDataStoreDaoMock).listAll();
        Mockito.doReturn(hostIDs).when(storageManagerMock).getUpHostsInPool(Mockito.anyLong());
        Mockito.doReturn(null).when(storageManagerMock).sendToPool(storagePoolMock, command);

        storageCollectorMock.executeCheckForPrimaryStorageStats();
        Mockito.verify(storagePoolMock, Mockito.never()).setCapacityBytes(Mockito.anyLong());
    }

    @Test
    public void executeCheckForPrimaryStorageStatsWithAnswerEqualFalseTest() throws StorageUnavailableException {
        List<StoragePoolVO> storagePoolsMock = new ArrayList<StoragePoolVO>();
        List<Long> hostIDs = new ArrayList<Long>();

        storagePoolsMock.add(storagePoolMock);
        hostIDs.add(hostIDMock);

        Mockito.doReturn(storagePoolsMock).when(primaryDataStoreDaoMock).listAll();
        Mockito.doReturn(hostIDs).when(storageManagerMock).getUpHostsInPool(Mockito.anyLong());
        Mockito.doReturn(answerMock).when(storageManagerMock).sendToPool(storagePoolMock, new GetStorageStatsCommand());
        Mockito.doReturn(false).when(answerMock).getResult();

        storageCollectorMock.executeCheckForPrimaryStorageStats();
        Mockito.verify(storagePoolMock, Mockito.never()).setCapacityBytes(Mockito.anyLong());
    }

    @Test
    public void executeCheckForPrimaryStorageStatsDoNotNeedUpdateTest() throws StorageUnavailableException {
        List<StoragePoolVO> storagePoolsMock = new ArrayList<StoragePoolVO>();
        List<Long> hostIDs = new ArrayList<Long>();

        GetStorageStatsCommand command = new GetStorageStatsCommand(storagePoolMock.getUuid(), storagePoolMock.getPoolType(), storagePoolMock.getPath());
        storagePoolsMock.add(storagePoolMock);
        hostIDs.add(hostIDMock);

        Mockito.doReturn(storagePoolsMock).when(primaryDataStoreDaoMock).listAll();
        Mockito.doReturn(hostIDs).when(storageManagerMock).getUpHostsInPool(Mockito.anyLong());
        Mockito.doReturn(storagePoolIDMock).when(storagePoolMock).getId();
        Mockito.doReturn(answerMock).when(storageManagerMock).sendToPool(storagePoolMock, command);
        Mockito.doReturn(true).when(answerMock).getResult();
        Mockito.doReturn(storageStatsMock).when(storageStatsHashMapMock).get(storagePoolIDMock);
        Mockito.when(answerMock.getCapacityBytes()).thenReturn(0l);
        Mockito.when(storagePoolMock.getCapacityBytes()).thenReturn(0l);

        storageCollectorMock.executeCheckForPrimaryStorageStats();
        Mockito.verify(storagePoolMock, Mockito.never()).setCapacityBytes(Mockito.anyLong());
    }

    @Test
    public void executeCheckForPrimaryStorageStatsNeededUpdateTest() throws StorageUnavailableException {
        List<StoragePoolVO> storagePoolsMock = new ArrayList<StoragePoolVO>();
        List<Long> hostIDs = new ArrayList<Long>();

        GetStorageStatsCommand command = new GetStorageStatsCommand(storagePoolMock.getUuid(), storagePoolMock.getPoolType(), storagePoolMock.getPath());
        storagePoolsMock.add(storagePoolMock);
        hostIDs.add(hostIDMock);

        Mockito.doReturn(storagePoolsMock).when(primaryDataStoreDaoMock).listAll();
        Mockito.doReturn(hostIDs).when(storageManagerMock).getUpHostsInPool(Mockito.anyLong());
        Mockito.doReturn(storagePoolIDMock).when(storagePoolMock).getId();
        Mockito.doReturn(answerMock).when(storageManagerMock).sendToPool(storagePoolMock, command);
        Mockito.doReturn(true).when(answerMock).getResult();
        Mockito.doReturn(storageStatsMock).when(storageStatsHashMapMock).get(storagePoolIDMock);
        Mockito.when(answerMock.getCapacityBytes()).thenReturn(1l);
        Mockito.when(storagePoolMock.getCapacityBytes()).thenReturn(0l);

        storageCollectorMock.executeCheckForPrimaryStorageStats();
        Mockito.verify(storagePoolMock, Mockito.times(1)).setCapacityBytes(Mockito.anyLong());
    }

    @Test
    public void executeCheckForSecondaryStorageWhenDataStoreIsEmptyTest() {
        List<DataStore> dataStoresMock = new ArrayList<DataStore>();

        Mockito.doReturn(dataStoresMock).when(dataStoreMgrMock).listImageStores();

        storageCollectorMock.executeCheckForSecondaryStorageStats();
        Mockito.verify(answerMock, Mockito.never()).getResult();
    }

    @Test
    public void executeCheckForSecondaryStorageWhenDataStoreUriIsNullTest (){
        List<DataStore> dataStoresMock = new ArrayList<DataStore>();
        dataStoresMock.add(dataStoreMock);

        Mockito.doReturn(dataStoresMock).when(dataStoreMgrMock).listImageStores();
        Mockito.doReturn(dataStoreIDMock).when(dataStoreMock).getId();
        Mockito.doReturn(null).when(dataStoreMock).getUri();

        storageCollectorMock.executeCheckForSecondaryStorageStats();
        Mockito.verify(answerMock, Mockito.never()).getResult();
    }

    @Test
    public void executeCheckForSecondaryStorageWhenEndPointHostIsNullTest (){
        List<DataStore> dataStoresMock = new ArrayList<DataStore>();
        dataStoresMock.add(dataStoreMock);

        Mockito.doReturn(dataStoresMock).when(dataStoreMgrMock).listImageStores();
        Mockito.doReturn(dataStoreIDMock).when(dataStoreMock).getId();
        Mockito.doReturn(dataStoreURIMock).when(dataStoreMock).getUri();
        Mockito.doReturn(null).when(imageStoreDetailsUtilMock).getNfsVersion(Mockito.anyLong());
        Mockito.doReturn(null).when(endPointSelectorMock).select(dataStoreMock);

        storageCollectorMock.executeCheckForSecondaryStorageStats();
        Mockito.verify(answerMock, Mockito.never()).getResult();
    }

    @Test
    public void executeCheckForSecondaryStorageWhenAnswerIsNullTest (){
        List<DataStore> dataStoresMock = new ArrayList<DataStore>();
        dataStoresMock.add(dataStoreMock);

        Mockito.doReturn(dataStoresMock).when(dataStoreMgrMock).listImageStores();
        Mockito.doReturn(dataStoreIDMock).when(dataStoreMock).getId();
        Mockito.doReturn(dataStoreURIMock).when(dataStoreMock).getUri();
        Mockito.doReturn(null).when(imageStoreDetailsUtilMock).getNfsVersion(Mockito.anyLong());
        Mockito.doReturn(hostEndPointMock).when(endPointSelectorMock).select(dataStoreMock);
        Mockito.doReturn(null).when(hostEndPointMock).sendMessage(answerMock);

        storageCollectorMock.executeCheckForSecondaryStorageStats();
        Mockito.verify(answerMock, Mockito.never()).getResult();
    }

    @Test
    public void executeCheckForSecondaryStorageWhenAnswerIsFalseTest (){
        List<DataStore> dataStoresMock = new ArrayList<DataStore>();
        dataStoresMock.add(dataStoreMock);

        Mockito.doReturn(dataStoresMock).when(dataStoreMgrMock).listImageStores();
        Mockito.doReturn(dataStoreIDMock).when(dataStoreMock).getId();
        Mockito.doReturn(dataStoreURIMock).when(dataStoreMock).getUri();
        Mockito.doReturn(null).when(imageStoreDetailsUtilMock).getNfsVersion(Mockito.anyLong());
        Mockito.doReturn(hostEndPointMock).when(endPointSelectorMock).select(dataStoreMock);
        Mockito.doReturn(answerMock).when(hostEndPointMock).sendMessage(new GetStorageStatsCommand());
        Mockito.doReturn(false).when(answerMock).getResult();

        storageCollectorMock.executeCheckForSecondaryStorageStats();
        Mockito.verify(storageCollectorMock, Mockito.never()).setSecondaryStorageStats(Mockito.anyLong(), Mockito.any(StorageStats.class));
    }

    @Test
    public void executeCheckForSecondaryStorageWhenAnswerIsTrueTest (){
        List<DataStore> dataStoresMock = new ArrayList<DataStore>();
        dataStoresMock.add(dataStoreMock);

        Mockito.doReturn(dataStoresMock).when(dataStoreMgrMock).listImageStores();
        Mockito.doReturn(dataStoreIDMock).when(dataStoreMock).getId();
        Mockito.doReturn(dataStoreURIMock).when(dataStoreMock).getUri();
        Mockito.doReturn(null).when(imageStoreDetailsUtilMock).getNfsVersion(Mockito.anyLong());
        Mockito.doReturn(hostEndPointMock).when(endPointSelectorMock).select(dataStoreMock);
        Mockito.doReturn(answerMock).when(hostEndPointMock).sendMessage(new GetStorageStatsCommand());
        Mockito.doReturn(true).when(answerMock).getResult();

        storageCollectorMock.executeCheckForSecondaryStorageStats();
        Mockito.verify(storageCollectorMock, Mockito.times(1)).setSecondaryStorageStats(Mockito.anyLong(), Mockito.any(StorageStats.class));
    }
}
package com.dangdang.ddframe.job.election;

import com.dangdang.ddframe.job.election.zookeeper.ElectionService;
import com.dangdang.ddframe.job.fixture.EmbedTestingServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.KillSession;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ElectionServiceTest {
    
    private static final String HOST_AND_PORT = "localhost:9898";
    
    private static final String ELECTION_PATH = "/election";
    
    private CuratorFramework client;
    
    private ElectionService service;
    
    @Mock
    private ElectionCandidate electionCandidate;
    
    @BeforeClass
    public static void init() throws InterruptedException {
        EmbedTestingServer.start();
    }
    
    @Before
    public void setUp() throws InterruptedException {
        client = CuratorFrameworkFactory.newClient(EmbedTestingServer.getConnectionString(), new RetryOneTime(2000));
        client.start();
        client.blockUntilConnected();
        MockitoAnnotations.initMocks(this);
        service = ElectionService.builder().identity(HOST_AND_PORT).electionPath(ELECTION_PATH)
                .client(client).electionCandidate(electionCandidate).build();
        service.startLeadership();
    }
    
    @After
    public void clean() {
        service.close();
        client.close();
    }
    
    @Test
    public void assertElectedLeader() throws Exception {
        assertTrue(service.isLeader());
        assertThat(service.getIdentity(), is(HOST_AND_PORT));
        service.abdicateLeadership();
        assertTrue(service.isLeader());
        assertThat(service.getIdentity(), is(HOST_AND_PORT));
        KillSession.kill(client.getZookeeperClient().getZooKeeper(), EmbedTestingServer.getConnectionString());
        assertTrue(service.isLeader());
        assertThat(service.getIdentity(), is(HOST_AND_PORT));
        verify(electionCandidate, times(3)).startLeadership();
        verify(electionCandidate, times(2)).stopLeadership();
    }
    
    
    @Test
    public void assertContend() throws Exception {
        ElectionCandidate anotherElectionCandidate = mock(ElectionCandidate.class);
        try (CuratorFramework anotherClient = CuratorFrameworkFactory.newClient(EmbedTestingServer.getConnectionString(), new RetryOneTime(2000));
             ElectionService anotherService = ElectionService.builder().identity("ANOTHER_CLIENT:9898").electionPath(ELECTION_PATH)
                     .client(anotherClient).electionCandidate(anotherElectionCandidate).build()) {
            anotherClient.start();
            anotherClient.blockUntilConnected();
            anotherService.startLeadership();
            ElectionService followService;
            CuratorFramework leaderClient;
            if (anotherService.isLeader()) {
                assertFalse(service.isLeader());
                leaderClient = anotherClient;
                followService = service;
            } else {
                assertTrue(service.isLeader());
                leaderClient = client;
                followService = anotherService;
            }
            KillSession.kill(leaderClient.getZookeeperClient().getZooKeeper(), EmbedTestingServer.getConnectionString());
            assertTrue(followService.isLeader());
        }
        verify(electionCandidate, atLeastOnce()).startLeadership();
        verify(anotherElectionCandidate, atLeastOnce()).startLeadership();
    }
}

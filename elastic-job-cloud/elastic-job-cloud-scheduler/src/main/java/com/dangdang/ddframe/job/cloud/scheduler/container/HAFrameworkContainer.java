/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.cloud.scheduler.container;

import com.dangdang.ddframe.job.cloud.scheduler.boot.env.BootstrapEnvironment;
import com.dangdang.ddframe.job.cloud.scheduler.mesos.FrameworkIDHolder;
import com.dangdang.ddframe.job.election.zookeeper.ElectionService;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.env.LocalHostService;
import com.dangdang.ddframe.job.util.zookeeper.CuratorFrameworks;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.Protos;

/**
 * 高可用框架容器.
 * 
 * @author gaohongtao
 */
@Slf4j
class HAFrameworkContainer extends AbstractFrameworkContainer {
    
    private static final LocalHostService LOCAL_HOST_SERVICE = new LocalHostService();
    
    private static final double ONE_WEEK_TIMEOUT = 60 * 60 * 24 * 7;
    
    private static final String ELECTION_NODE = "/ha";
    
    private final AbstractFrameworkContainer delegate;
    
    private final ElectionService electionService;
    
    HAFrameworkContainer(final CoordinatorRegistryCenter regCenter, final Protos.FrameworkInfo.Builder frameworkInfoBuilder) {
        super(regCenter);
        FrameworkIDHolder.setRegCenter(regCenter);
        delegate = new StandaloneFrameworkContainer(regCenter, FrameworkIDHolder.supply(frameworkInfoBuilder).setFailoverTimeout(ONE_WEEK_TIMEOUT).build(), this);
        electionService = ElectionService.builder().identity(String.format("%s:%d", LOCAL_HOST_SERVICE.getHostName(), BootstrapEnvironment.getInstance().getRestfulServerConfiguration().getPort()))
                .client(CuratorFrameworks.newClient(BootstrapEnvironment.getInstance().getZookeeperConfiguration())).electionPath(ELECTION_NODE)
                .electionCandidate(delegate).build();
    }
    
    @Override
    public void start() throws Exception {
        electionService.startLeadership();
        log.info("Elastic job: The framework {} leader", electionService.isLeader() ? "is" : "is not");
    }
    
    @Override
    public void pause() {
        log.info("Elastic job: HA container pause");
        electionService.abdicateLeadership();
    }
    
    @Override
    public void resume() {
        log.info("Elastic job: HA container resume");
        delegate.resume();
    }
    
    @Override
    public void shutdown() {
        log.info("Elastic job: HA container shutdown");
        electionService.close();
    }
}

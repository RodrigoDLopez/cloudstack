/**
 *  Copyright (C) 2010 Cloud.com, Inc.  All rights reserved.
 * 
 * This software is licensed under the GNU General Public License v3 or later.
 * 
 * It is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */

package com.cloud.api.commands;

import org.apache.log4j.Logger;

import com.cloud.api.ApiConstants;
import com.cloud.api.ApiResponseHelper;
import com.cloud.api.BaseCmd;
import com.cloud.api.Implementation;
import com.cloud.api.Parameter;
import com.cloud.api.ServerApiException;
import com.cloud.api.response.ResourceLimitResponse;
import com.cloud.configuration.ResourceLimit;
import com.cloud.exception.ConcurrentOperationException;
import com.cloud.exception.InsufficientAddressCapacityException;
import com.cloud.exception.InsufficientCapacityException;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.exception.PermissionDeniedException;

@Implementation(description="Updates resource limits for an account or domain.")
public class UpdateResourceLimitCmd extends BaseCmd {
    public static final Logger s_logger = Logger.getLogger(UpdateResourceLimitCmd.class.getName());

    private static final String s_name = "updateresourcelimitresponse";


    /////////////////////////////////////////////////////
    //////////////// API parameters /////////////////////
    /////////////////////////////////////////////////////

    @Parameter(name=ApiConstants.ACCOUNT, type=CommandType.STRING, description="Update resource for a specified account. Must be used with the domainId parameter.")
    private String accountName;

    @Parameter(name=ApiConstants.DOMAIN_ID, type=CommandType.LONG, description="Update resource limits for all accounts in specified domain. If used with the account parameter, updates resource limits for a specified account in specified domain.")
    private Long domainId;

    @Parameter(name=ApiConstants.MAX, type=CommandType.LONG, description="	Maximum resource limit.")
    private Long max;

    @Parameter(name=ApiConstants.RESOURCE_TYPE, type=CommandType.INTEGER, required=true, description="Type of resource to update. Values are 0, 1, 2, 3, and 4. 0 - Instance. Number of instances a user can create. " +
    																					"1 - IP. Number of public IP addresses a user can own. " +
    																					"2 - Volume. Number of disk volumes a user can create." +
    																					"3 - Snapshot. Number of snapshots a user can create." +
    																					"4 - Template. Number of templates that a user can register/create.")
    private Integer resourceType;

    /////////////////////////////////////////////////////
    /////////////////// Accessors ///////////////////////
    /////////////////////////////////////////////////////

    public String getAccountName() {
        return accountName;
    }

    public Long getDomainId() {
        return domainId;
    }

    public Long getMax() {
        return max;
    }

    public Integer getResourceType() {
        return resourceType;
    }

    /////////////////////////////////////////////////////
    /////////////// API Implementation///////////////////
    /////////////////////////////////////////////////////
    
    @Override
    public String getName() {
        return s_name;
    }

    @Override
    public void execute() throws ServerApiException, InvalidParameterValueException, PermissionDeniedException, InsufficientAddressCapacityException, InsufficientCapacityException, ConcurrentOperationException{
        ResourceLimit result = _accountService.updateResourceLimit(this);
        ResourceLimitResponse response = ApiResponseHelper.createResourceLimitResponse(result);
        response.setResponseName(getName());
        this.setResponseObject(response);
    }
}

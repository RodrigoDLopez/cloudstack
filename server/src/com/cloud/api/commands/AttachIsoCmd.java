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
import com.cloud.api.ApiDBUtils;
import com.cloud.api.BaseAsyncCmd;
import com.cloud.api.BaseCmd;
import com.cloud.api.Implementation;
import com.cloud.api.Parameter;
import com.cloud.api.ServerApiException;
import com.cloud.api.response.IsoVmResponse;
import com.cloud.event.EventTypes;
import com.cloud.exception.ConcurrentOperationException;
import com.cloud.exception.InsufficientAddressCapacityException;
import com.cloud.exception.InsufficientCapacityException;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.exception.PermissionDeniedException;
import com.cloud.storage.VMTemplateVO;
import com.cloud.user.Account;
import com.cloud.vm.VMInstanceVO;

@Implementation(description="Attaches an ISO to a virtual machine.")
public class AttachIsoCmd extends BaseAsyncCmd {
    public static final Logger s_logger = Logger.getLogger(AttachIsoCmd.class.getName());

    private static final String s_name = "attachisoresponse";

    /////////////////////////////////////////////////////
    //////////////// API parameters /////////////////////
    /////////////////////////////////////////////////////

    @Parameter(name=ApiConstants.ID, type=CommandType.LONG, required=true, description="the ID of the ISO file")
    private Long id;

    @Parameter(name=ApiConstants.VIRTUAL_MACHINE_ID, type=CommandType.LONG, required=true, description="the ID of the virtual machine")
    private Long virtualMachineId;


    /////////////////////////////////////////////////////
    /////////////////// Accessors ///////////////////////
    /////////////////////////////////////////////////////

    public Long getId() {
        return id;
    }

    public Long getVirtualMachineId() {
        return virtualMachineId;
    }


    /////////////////////////////////////////////////////
    /////////////// API Implementation///////////////////
    /////////////////////////////////////////////////////

    @Override
    public String getName() {
        return s_name;
    }
    
    @Override
    public long getAccountId() {
        VMTemplateVO iso = ApiDBUtils.findTemplateById(getId());
        if (iso == null) {
            return Account.ACCOUNT_ID_SYSTEM; // bad id given, parent this command to SYSTEM so ERROR events are tracked
        }
        return iso.getAccountId();
    }

    @Override
    public String getEventType() {
        return EventTypes.EVENT_ISO_ATTACH;
    }

    @Override
    public String getEventDescription() {
        return  "attaching ISO: " + getId() + " to vm: " + getVirtualMachineId();
    }
    
    @Override
    public void execute() throws ServerApiException, InvalidParameterValueException, PermissionDeniedException, InsufficientAddressCapacityException, InsufficientCapacityException, ConcurrentOperationException{
        boolean result = _templateMgr.attachIso(this);
        if (result) {
            IsoVmResponse response = new IsoVmResponse();
            VMTemplateVO iso = ApiDBUtils.findTemplateById(id);
            VMInstanceVO vmInstance = ApiDBUtils.findVMInstanceById(virtualMachineId);
            
            response.setId(id);
            response.setName(iso.getName());
            response.setDisplayText(iso.getDisplayText());
            response.setOsTypeId(iso.getGuestOSId());
            response.setOsTypeName(ApiDBUtils.findGuestOSById(iso.getGuestOSId()).getName());
            response.setVirtualMachineId(virtualMachineId);
            response.setVirtualMachineName(vmInstance.getHostName());
            response.setVirtualMachineState(vmInstance.getState().toString());            
            response.setResponseName(getName());
            response.setObjectName("iso");
            
            this.setResponseObject(response);
        } else {
            throw new ServerApiException(BaseCmd.INTERNAL_ERROR, "Failed to attach iso");
        }
    }
}

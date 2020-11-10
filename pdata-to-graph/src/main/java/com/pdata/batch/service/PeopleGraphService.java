package com.pdata.batch.service;

import java.util.List;

import com.pdata.batch.dto.PeopleDTO;
import com.pdata.batch.dto.PeopleDTO2;

public interface PeopleGraphService {
	public void bulkUpload(List<PeopleDTO> peopleDTO) throws Exception;

	public void bulkUploadToGraph(List<? extends PeopleDTO> items)  throws Exception;
	
	public void bulkUploadToGraph2(List<? extends PeopleDTO2> items)  throws Exception;
	
	public void bulkImportUsingStoredProcedure(List<? extends PeopleDTO2> peopleDTOList) throws Exception;
}

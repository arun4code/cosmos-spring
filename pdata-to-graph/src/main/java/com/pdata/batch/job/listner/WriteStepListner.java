package com.pdata.batch.job.listner;

import java.util.List;

import org.springframework.batch.core.ItemWriteListener;

import com.pdata.batch.dto.PeopleDTO;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WriteStepListner implements ItemWriteListener<PeopleDTO> {

	@Override
	public void beforeWrite(List<? extends PeopleDTO> items) {
		log.info("Total count---before write------ " + items.size());
	}

	@Override
	public void afterWrite(List<? extends PeopleDTO> items) {

	}

	@Override
	public void onWriteError(Exception exception, List<? extends PeopleDTO> items) {
		log.info("error while writing--- " + items.size());
	}
}

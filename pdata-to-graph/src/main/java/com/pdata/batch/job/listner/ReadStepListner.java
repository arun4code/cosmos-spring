package com.pdata.batch.job.listner;

import org.springframework.batch.core.ItemReadListener;

import com.pdata.batch.model.People;

public class ReadStepListner implements ItemReadListener<People>{

	@Override
	public void beforeRead() {
		
	}

	@Override
	public void afterRead(People item) {

	}

	@Override
	public void onReadError(Exception ex) {
		
	}

}

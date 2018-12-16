package com.bazaarvoice.emodb.web.resources.sor;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import io.dropwizard.jersey.params.AbstractParam;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

public class DeltaParam extends AbstractParam<Delta> {

    public DeltaParam(String input) {
        super(input);
    }

    @Override
    protected String errorMessage(Exception e) {
        return "Invalid json-delta string";
    }

    @Override
    protected Delta parse(String input) throws Exception {
        return Deltas.fromString(input);
    }
}

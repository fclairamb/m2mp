package org.m2mp.es;

import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 *
 * @author Florent Clairambault
 */
public class EsSearchRequest {

	private QueryBuilder query;

	public EsSearchRequest setQuery(QueryBuilder query) {
		this.query = query;
		return this;
	}
	private final List<SortBuilder> sorts = new ArrayList<>();

	public EsSearchRequest addSort(String fieldName) {
		addSort(SortBuilders.fieldSort(fieldName));
		return this;
	}

	public EsSearchRequest addSort(String fieldName, boolean order) {
		addSort(SortBuilders.fieldSort(fieldName).order(order ? SortOrder.ASC : SortOrder.DESC));
		return this;
	}

	public EsSearchRequest addSort(SortBuilder sb) {
		sorts.add(sb);
		return this;
	}
	private final List<AbstractFacetBuilder> facets = new ArrayList<>();

	public EsSearchRequest addFacetsTerms(String fieldName) {
		facets.add(FacetBuilders.termsFacet(fieldName).field(fieldName));
		return this;
	}
	private int from = -1;

	public EsSearchRequest setFrom(int from) {
		this.from = from;
		return this;
	}
	private int size = -1;

	public EsSearchRequest setSize(int size) {
		this.size = size;
		return this;
	}

	public SearchRequestBuilder getSearchRequest() {
		SearchRequestBuilder search = new SearchRequestBuilder(null);
		search.setQuery(query);
		for (AbstractFacetBuilder afb : facets) {
			search.addFacet(afb);
		}
		for (SortBuilder sb : sorts) {
			search.addSort(sb);
		}
		if (size != -1) {
			search.setSize(size);
		}
		if (from != -1) {
			search.setFrom(from);
		}
		return search;
	}

	@Override
	public String toString() {
		return getSearchRequest().toString();
	}
}

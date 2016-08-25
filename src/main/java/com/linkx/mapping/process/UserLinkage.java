package com.linkx.mapping.process;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;

import java.util.List;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/11.
 */
public class UserLinkage {
    // final selected user id
    String userId;

    // identity linkage list
    List<Linkage> linkages;

    public UserLinkage(String userId, List<Linkage> linkages) {
        this.userId = userId;
        this.linkages = linkages;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public List<Linkage> getLinkages() {
        return linkages;
    }

    public void setLinkages(List<Linkage> linkages) {
        this.linkages = linkages;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("userId", userId)
                .add("linkages", '[' + Joiner.on(',').join(linkages) + ']')
                .toString();
    }
}

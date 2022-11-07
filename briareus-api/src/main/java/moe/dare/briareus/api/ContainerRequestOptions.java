package moe.dare.briareus.api;

/**
 * Class to represent a single container request options
 * (See @link org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest)
 */
public class ContainerRequestOptions {
    private final String[] nodes;
    private final String[] racks;
    private final String nodeLabelsExpression;
    private final boolean relaxLocality;

    public static ContainerRequestOptionsBuilder newBuilder() {
        return new ContainerRequestOptionsBuilder();
    }

    private ContainerRequestOptions(ContainerRequestOptionsBuilder builder) {
        this.nodes = builder.nodes;
        this.racks = builder.racks;
        this.nodeLabelsExpression = builder.nodeLabelsExpression;
        this.relaxLocality = builder.relaxLocality;
    }

    /**
     * The array of hosts to request that the containers are placed on
     * @return array of nodes
     */
    public String[] getNodes() {
        return nodes;
    }

    /**
     * The array of racks to request that the containers are placed on.
     * The racks corresponding to any hosts requested will be automatically added to this list
     * @return array of racks
     */
    public String[] getRacks() {
        return racks;
    }

    /**
     * The node labels to allocate resource, now we only support asking for only a single node label
     * @return node labels
     */
    public String getNodeLabelsExpression() {
        return nodeLabelsExpression;
    }

    /**
     * If true, containers for this request may be assigned on hosts and racks other than the ones explicitly requested
     * @return value of relaxLocality
     */
    public boolean getRelaxLocality() {
        return relaxLocality;
    }

    public static class ContainerRequestOptionsBuilder {
        private String[] nodes;
        private String[] racks;
        private String nodeLabelsExpression;
        private boolean relaxLocality = true;

        private ContainerRequestOptionsBuilder() {
        }

        /**
         * The array of hosts to request that the containers are placed on
         * @param nodes array of nodes
         * @return this instance for chaining
         */
        public ContainerRequestOptionsBuilder nodes(String[] nodes) {
            this.nodes = nodes;
            return this;
        }

        /**
         * The array of racks to request that the containers are placed on
         * @param racks array of nodes
         * @return this instance for chaining
         */
        public ContainerRequestOptionsBuilder racks(String[] racks) {
            this.racks = racks;
            return this;
        }

        /**
         * The node labels to allocate resource, now we only support asking for only a single node label
         * @param nodeLabelsExpression node labels expression
         * @return this instance for chaining
         */
        public ContainerRequestOptionsBuilder nodeLabelsExpression(String nodeLabelsExpression) {
            this.nodeLabelsExpression = nodeLabelsExpression;
            return this;
        }

        /**
         * If true, containers for this request may be assigned on hosts and racks other than the ones explicitly requested
         * @param relaxLocality value of relaxLocality flag
         * @return this instance for chaining
         */
        public ContainerRequestOptionsBuilder relaxLocality(boolean relaxLocality) {
            this.relaxLocality = relaxLocality;
            return this;
        }

        public ContainerRequestOptions build() {
            return new ContainerRequestOptions(this);
        }
    }
}
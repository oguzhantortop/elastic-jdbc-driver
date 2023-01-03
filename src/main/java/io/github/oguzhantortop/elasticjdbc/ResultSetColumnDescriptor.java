package io.github.oguzhantortop.elasticjdbc;

public class ResultSetColumnDescriptor {

    private String name;
    private String type;
    private String label;

    public ResultSetColumnDescriptor(String name, String type, String label) {
        this.name = name;
        this.type = type;
        this.label = label;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getLabel() {
        return label;
    }
}

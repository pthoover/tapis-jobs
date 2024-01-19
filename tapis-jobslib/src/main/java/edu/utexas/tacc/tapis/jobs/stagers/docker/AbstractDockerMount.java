package edu.utexas.tacc.tapis.jobs.stagers.docker;

abstract class AbstractDockerMount
 implements DockerMount
{
    // Fields.
    protected final MountType type;
    protected String source;
    protected String target;
    
    // Constructor.
    protected AbstractDockerMount(MountType type)
    {this.type = type;}
    
    // Accessors.
    @Override
    public MountType getType() {
        return type;
    }
    @Override
    public String getSource() {
        return source;
    }
    @Override
    public void setSource(String source) {
        this.source = source;
    }
    @Override
    public String getTarget() {
        return target;
    }
    @Override
    public void setTarget(String target) {
        this.target = target;
    }
}

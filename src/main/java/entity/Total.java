package entity;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Total {
    private Cpu cpu;
    private Memory memory;
    private String event_time;

    public Total(Cpu cpu, Memory memory, String event_time) {
        this.cpu = cpu;
        this.memory = memory;
        this.event_time = event_time;
    }

    public Total() {
    }
}

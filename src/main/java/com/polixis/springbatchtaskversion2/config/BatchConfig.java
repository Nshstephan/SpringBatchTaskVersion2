package com.polixis.springbatchtaskversion2.config;

import com.polixis.springbatchtaskversion2.dto.EmployeeDto;
import com.polixis.springbatchtaskversion2.model.Employee;
import com.polixis.springbatchtaskversion2.process.ArchiveResourceItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.util.Comparator;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Value("${zip-file-path}")
    private Resource resource;


    /**
     * The multiResourceItemReader() method is used to read multiple CSV files
     */
    @Bean
    public MultiResourceItemReader<EmployeeDto> multiResourceItemReader() {
        ArchiveResourceItemReader<EmployeeDto> resourceItemReader = new ArchiveResourceItemReader<>();
        resourceItemReader.setResource(resource);
        resourceItemReader.setDelegate(reader());
        resourceItemReader.setComparator(Comparator.comparing(Resource::getDescription));
        return resourceItemReader;
    }

    /**
     * The reader() method is used to read the data from the CSV file
     */
    @Bean
    public FlatFileItemReader<EmployeeDto> reader() {
        FlatFileItemReader<EmployeeDto> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper<>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames("firstName", "lastName", "date");
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                setTargetType(EmployeeDto.class);
            }});
        }});
        return reader;
    }

    /**
     * The writer() method is used to write a data into the SQL.
     */
    @Bean
    public JdbcBatchItemWriter<Employee> writer()
    {
        JdbcBatchItemWriter<Employee> writer = new JdbcBatchItemWriter<Employee>();
        writer.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<Employee>());
        writer.setSql("INSERT INTO EMPLOYEE (FIRSTNAME, LASTNAME, DATE) VALUES (:firstName, :lastName, :date)");
        writer.setDataSource(dataSource);
        return writer;
    }

    @Bean
    public Job insertEmployeeJob()
    {
        return jobBuilderFactory.get("importEmployeeJob").incrementer(new RunIdIncrementer())
                .start(step1()).build();
    }

    @Bean
    public Step step1()
    {
        return stepBuilderFactory.get("step1").<Employee, Employee>chunk(5)
                .reader(multiResourceItemReader())
                .writer(writer()).build();
    }
}
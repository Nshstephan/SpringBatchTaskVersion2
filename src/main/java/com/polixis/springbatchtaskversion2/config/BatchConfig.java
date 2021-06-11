package com.polixis.springbatchtaskversion2.config;

import com.polixis.springbatchtaskversion2.dto.EmployeeDto;
import com.polixis.springbatchtaskversion2.process.ArchiveResourceItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
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
        //Create reader instance
        FlatFileItemReader<EmployeeDto> reader = new FlatFileItemReader<>();

        //Set number of lines to skips.
        reader.setLinesToSkip(1);

        //Configure how each line will be parsed and mapped to different values
        reader.setLineMapper(new DefaultLineMapper<>() {
            {
                //3 columns in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("firstName", "lastName", "date");
                    }
                });
                //Set values in EmployeeDto class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<>() {
                    {
                        setTargetType(EmployeeDto.class);
                    }
                });
            }
        });

        return reader;
    }

    /**
     * The writer() method is used to write a data into the SQL.
     */
    @Bean
    public JdbcBatchItemWriter<EmployeeDto> writer() {
        JdbcBatchItemWriter<EmployeeDto> writer = new JdbcBatchItemWriter<EmployeeDto>();
        writer.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<EmployeeDto>());
        writer.setSql("INSERT INTO EMPLOYEE (FIRSTNAME, LASTNAME, DATE) VALUES (:firstName, :lastName, :date)");
        writer.setDataSource(dataSource);

        return writer;
    }

    @Bean
    public Job insertEmployeeJob() {
        return jobBuilderFactory.get("importEmployeeJob")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1").<EmployeeDto, EmployeeDto>chunk(50)
                .reader(multiResourceItemReader())
                .writer(writer())
                .build();
    }
}
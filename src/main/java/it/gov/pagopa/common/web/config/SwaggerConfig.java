package it.gov.pagopa.common.web.config;

import com.fasterxml.classmate.TypeResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.Ordered;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import springfox.documentation.builders.AlternateTypeBuilder;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.schema.CachingModelProvider;
import springfox.documentation.schema.ModelProvider;
import springfox.documentation.schema.ModelSpecification;
import springfox.documentation.schema.ModelSpecificationProvider;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spi.schema.contexts.ModelContext;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.lang.reflect.Type;
import java.time.LocalTime;
import java.util.Optional;
import java.util.function.Predicate;

import static springfox.documentation.schema.AlternateTypeRules.newRule;

/**
 * The Class SwaggerConfig.
 */
@Configuration
@EnableSwagger2
public class SwaggerConfig {

	/** The title. */
	@Value("${swagger.title:${spring.application.name}}")
	private String title;

	/** The description. */
	@Value("${swagger.description:Api and Models}")
	private String description;

	/** The version. */
	@Value("${swagger.version:${spring.application.version}}")
	private String version;

	/**
	 * Swagger spring.
	 *
	 * @param typeResolver the type resolver
	 * @return the docket
	 */
	@Bean
	public Docket swaggerSpringPlugin(@Autowired TypeResolver typeResolver) {
		return (new Docket(DocumentationType.OAS_30)).select().apis(RequestHandlerSelectors.any())
				.apis(Predicate.not(RequestHandlerSelectors.basePackage("org.springframework.boot")))
				.apis(Predicate.not(RequestHandlerSelectors.basePackage("org.springframework.hateoas"))).build()
				.alternateTypeRules(
						newRule(typeResolver.resolve(Pageable.class), pageableMixin(), Ordered.HIGHEST_PRECEDENCE))
				.directModelSubstitute(LocalTime.class, String.class)
				.apiInfo(this.metadata());
	}

	/**
	 * Metadata.
	 *
	 * @return the api info
	 */
	private ApiInfo metadata() {
		return (new ApiInfoBuilder()).title(this.title).description(this.description).version(this.version).build();
	}

	private Type pageableMixin() {
		return new AlternateTypeBuilder()
				.fullyQualifiedClassName(String.format("%s.generated.%s", Pageable.class.getPackage().getName(),
						Pageable.class.getSimpleName()))
				.property(b->b.name("page").type(Integer.class).canRead(true).canWrite(true))
				.property(b->b.name("size").type(Integer.class).canRead(true).canWrite(true))
				.property(b->b.name("sort").type(String.class).canRead(true).canWrite(true))
				.build();
	}

	@Bean
	@Primary
	@Qualifier("cachedModels")
	public ModelSpecificationProvider customModelSpecificationProvider(
			@SuppressWarnings({"squid:S1874", "deprecation"}) // suppressed, caused used by the library service
			@Qualifier("default") ModelProvider delegate,
			@Qualifier("default") ModelSpecificationProvider specificationDelegate){
		return new CachingModelProvider(delegate, specificationDelegate){
			@Override
			public Optional<ModelSpecification> modelSpecificationsFor(ModelContext modelContext) {
				Optional<ModelSpecification> spec = super.modelSpecificationsFor(modelContext);
				if(Page.class.equals(modelContext.getType().getErasedType())){
					spec.flatMap(ModelSpecification::getCompound)
							.ifPresent(m->m
									.getProperties()
									.removeIf(f->"pageable".equals(f.getName())));
					return spec;
				} else {
					return spec;
				}
			}
		};
	}

}
